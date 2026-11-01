#include "storage/chunk_store/ChunkStore.h"

#include "storage/chunk_store/ChunkStoreInternal.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/types.h>
#include <unistd.h>

namespace object_storage {
namespace fs = std::filesystem;

namespace detail {

constexpr uint32_t kFormatVersion = 1;
std::atomic<uint64_t> gMoveSequence{0};

Status Error(ErrorCode code, const std::string& operation)
{
    return {code, operation + ": " + std::strerror(errno)};
}

Status IoError(const std::string& operation)
{
    return Error(errno == ENOSPC ? ErrorCode::kNoSpace : ErrorCode::kIoError, operation);
}

// 安全地解析无符号整数，防止溢出
bool ParseUnsigned(const std::string& text, uint64_t* value)
{
    if (text.empty() || (text.size() > 1 && text[0] == '0')) return false;
    uint64_t result = 0;
    for (char c : text) {
        if (c < '0' || c > '9') return false;
        const unsigned digit = static_cast<unsigned>(c - '0');
        if (result > (UINT64_MAX - digit) / 10) return false;
        result = result * 10 + digit;
    }
    *value = result;
    return true;
}

// 读取通用元数据
Result<std::map<std::string, std::string>> ReadFields(const fs::path& path, size_t limit)
{
    std::error_code ec;
    const uintmax_t size = fs::file_size(path, ec);
    if (ec) return Status(ErrorCode::kIoError, "cannot stat metadata: " + ec.message());
    if (size == 0 || size > limit) {
        return Status(ErrorCode::kCorrupt, "metadata size is invalid");
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) return Status(ErrorCode::kIoError, "cannot open metadata");
    std::string contents(static_cast<size_t>(size), '\0');
    input.read(contents.data(), static_cast<std::streamsize>(contents.size()));
    if (!input || contents.back() != '\n') {
        return Status(ErrorCode::kCorrupt, "metadata is truncated");
    }
    std::map<std::string, std::string> fields;
    std::istringstream lines(contents);
    std::string line;
    while (std::getline(lines, line)) {
        const size_t equal = line.find('=');
        if (equal == std::string::npos || equal == 0 || equal + 1 == line.size()) {
            return Status(ErrorCode::kCorrupt, "invalid metadata line");
        }
        const std::string key = line.substr(0, equal);
        const std::string value = line.substr(equal + 1);
        if (!fields.emplace(key, value).second) {
            return Status(ErrorCode::kCorrupt, "duplicate metadata field");
        }
    }
    return fields;
}

std::string DescriptorText(const ChunkDescriptor& descriptor)
{
    std::ostringstream out;
    out << "format_version=" << descriptor.formatVersion << '\n'
        << "chunk_id=" << descriptor.chunkId << '\n'
        << "object_id=" << descriptor.objectId << '\n'
        << "object_version=" << descriptor.objectVersion << '\n'
        << "index=" << descriptor.index << '\n'
        << "logical_offset=" << descriptor.logicalOffset << '\n'
        << "size=" << descriptor.size << '\n'
        << "checksum_algorithm=sha256\n"
        << "checksum=" << descriptor.checksum.value << '\n'
        << "generation=" << descriptor.generation << '\n';
    return out.str();
}

Result<ChunkDescriptor> ParseDescriptorFields(const std::map<std::string, std::string>& f)
{
    static const std::set<std::string> expected = {
        "format_version", "chunk_id", "object_id", "object_version", "index",
        "logical_offset", "size", "checksum_algorithm", "checksum", "generation"};
    std::set<std::string> actual;
    for (const auto& item : f) actual.insert(item.first);
    if (actual != expected || f.at("checksum_algorithm") != "sha256") {
        return Status(ErrorCode::kCorrupt, "metadata fields are invalid");
    }

    uint64_t format = 0, version = 0, index = 0, logical = 0, size = 0, generation = 0;
    if (!ParseUnsigned(f.at("format_version"), &format) || format != kFormatVersion ||
        !ParseUnsigned(f.at("object_version"), &version) ||
        !ParseUnsigned(f.at("index"), &index) || index > UINT32_MAX ||
        !ParseUnsigned(f.at("logical_offset"), &logical) ||
        !ParseUnsigned(f.at("size"), &size) ||
        !ParseUnsigned(f.at("generation"), &generation) || generation == 0 ||
        !IsCanonicalId(f.at("chunk_id")) || !IsCanonicalId(f.at("object_id"))) {
        return Status(ErrorCode::kCorrupt, "metadata values are invalid");
    }
    ChunkDescriptor descriptor;
    descriptor.formatVersion = static_cast<uint32_t>(format);
    descriptor.chunkId = f.at("chunk_id");
    descriptor.objectId = f.at("object_id");
    descriptor.objectVersion = version;
    descriptor.index = static_cast<uint32_t>(index);
    descriptor.logicalOffset = logical;
    descriptor.size = size;
    descriptor.checksum = {ChecksumAlgorithm::kSha256, f.at("checksum")};
    descriptor.generation = generation;
    Status checksumStatus = ValidateChecksum(descriptor.checksum);
    if (!checksumStatus.ok()) return Status(ErrorCode::kCorrupt, checksumStatus.message());
    return descriptor;
}

// 从path解析ChunkDescriptor，限制元数据大小为limit
Result<ChunkDescriptor> ParseDescriptor(const fs::path& path, size_t limit)
{
    auto parsed = ReadFields(path, limit);
    if (!parsed.ok()) return parsed.status();
    return ParseDescriptorFields(parsed.value());
}

std::string UploadText(const UploadMetadata& upload)
{
    std::ostringstream out;
    out << DescriptorText(upload.descriptor)
        << "upload_id=" << upload.uploadId << '\n'
        << "durable_bytes=" << upload.durableBytes << '\n'
        << "expires_at_micros=" << upload.expiresAtMicros << '\n';
    return out.str();
}

Result<UploadMetadata> ParseUpload(const fs::path& path, size_t limit)
{
    auto parsed = ReadFields(path, limit);
    if (!parsed.ok()) return parsed.status();
    auto fields = parsed.value();
    const auto uploadIt = fields.find("upload_id");
    const auto durableIt = fields.find("durable_bytes");
    const auto expiresIt = fields.find("expires_at_micros");
    if (uploadIt == fields.end() || durableIt == fields.end() || expiresIt == fields.end() ||
        !IsCanonicalId(uploadIt->second)) {
        return Status(ErrorCode::kCorrupt, "upload metadata fields are invalid");
    }
    uint64_t durable = 0, expires = 0;
    if (!ParseUnsigned(durableIt->second, &durable) ||
        !ParseUnsigned(expiresIt->second, &expires) || expires > INT64_MAX) {
        return Status(ErrorCode::kCorrupt, "upload metadata values are invalid");
    }
    const std::string uploadId = uploadIt->second;
    fields.erase("upload_id");
    fields.erase("durable_bytes");
    fields.erase("expires_at_micros");
    auto parsedDescriptor = ParseDescriptorFields(fields);
    if (!parsedDescriptor.ok()) return parsedDescriptor.status();
    if (durable > parsedDescriptor.value().size) {
        return Status(ErrorCode::kCorrupt, "durable offset exceeds expected size");
    }
    return UploadMetadata{uploadId, parsedDescriptor.value(), durable,
                          static_cast<int64_t>(expires)};
}

// 安全地将所有内容写入缓冲区
Status WriteAll(int fd, const std::string& contents)
{
    size_t offset = 0;
    while (offset < contents.size()) {
        const ssize_t n = ::write(fd, contents.data() + offset, contents.size() - offset);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return IoError("write metadata");
        offset += static_cast<size_t>(n);
    }
    return Status::Ok();
}

Status FsyncDirectory(const fs::path& directory)
{
    const int fd = ::open(directory.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECTORY);
    if (fd < 0) return IoError("open directory for fsync");
    const int result = ::fsync(fd);
    const int saved = errno;
    ::close(fd);

    if (result != 0) {
        errno = saved;
        return IoError("fsync directory");
    }
    return Status::Ok();
}

int DataSync(int fd)
{
#if defined(__linux__)
    return ::fdatasync(fd);
#else
    return ::fsync(fd);
#endif
}

// 原子更新 meta文件，先写入临时文件，再重命名覆盖目标文件
Status AtomicWrite(const fs::path& path, const std::string& contents)
{
    const fs::path temporary = path.string() + ".tmp"; // 创建path.tmp临时文件
    const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0640);
    if (fd < 0) return IoError("open metadata temporary file");
    Status status = WriteAll(fd, contents); // 写入内容到临时文件
    if (status.ok() && ::fsync(fd) != 0) status = IoError("fsync metadata");
    const int saved = errno;
    ::close(fd);
    errno = saved;
    if (!status.ok()) return status;
    if (::rename(temporary.c_str(), path.c_str()) != 0) return IoError("rename metadata");
    return FsyncDirectory(path.parent_path());
}


Status EnsureDirectory(const fs::path& directory)
{
    std::error_code ec;
    const bool exists = fs::exists(directory, ec);
    if (ec) {
        return Status(ErrorCode::kIoError,
                    "check storage directory: " + ec.message());
    }
    if (exists) {
        if (!fs::is_directory(directory, ec) || ec) {
            return Status(ErrorCode::kIoError,
                        "storage path is not a directory");
        }

        return Status::Ok();
    }

    if (::mkdir(directory.c_str(), 0750) != 0) {
        const int saved = errno;
        if (saved == EEXIST && fs::is_directory(directory, ec) && !ec) {  // 检查竟态
            return Status::Ok();
        }
        errno = saved;
        return IoError("create storage directory");
    }
    return FsyncDirectory(directory.parent_path());
}

Status EnsureFinalParent(const std::string& root, const std::string& chunkId)
{
    const fs::path final = FinalPath(root, chunkId);
    Status first = EnsureDirectory(final.parent_path().parent_path());
    if (!first.ok()) return first;
    return EnsureDirectory(final.parent_path());
}

// 恢复可续传前缀，持久化同步
Status TruncateAndSync(int fd, uint64_t length)
{
    if (::ftruncate(fd, static_cast<off_t>(length)) != 0) return IoError("truncate partial chunk");
    return DataSync(fd) == 0 ? Status::Ok() : IoError("sync truncated partial chunk");
}

Status ValidateDescriptor(const ChunkDescriptor& descriptor, uint64_t maxChunkSize)
{
    if (!IsCanonicalId(descriptor.chunkId) || !IsCanonicalId(descriptor.objectId)) {
        return {ErrorCode::kInvalidArgument, "chunk_id and object_id must be canonical UUIDs"};
    }
    if (descriptor.generation == 0 || descriptor.formatVersion != kFormatVersion ||
        descriptor.size > maxChunkSize) {
        return {ErrorCode::kInvalidArgument, "descriptor generation, format, or size is invalid"};
    }
    return ValidateChecksum(descriptor.checksum);
}

// 加载meta和data文件，检查是否存在，大小是否正确，返回ChunkDescriptor
Result<ChunkDescriptor> LoadCommitted(const std::string& root, size_t metadataLimit,
                                      const std::string& chunkId)
{
    const fs::path directory = FinalPath(root, chunkId);
    auto descriptor = ParseDescriptor(directory / "meta", metadataLimit);
    if (!descriptor.ok()) {
        std::error_code ec;
        if (!fs::exists(directory, ec) && !ec) {
            return Status(ErrorCode::kNotFound, "chunk not found");
        }
        return descriptor.status();
    }
    if (descriptor.value().chunkId != chunkId) {
        return Status(ErrorCode::kCorrupt, "chunk path and metadata disagree");
    }
    std::error_code ec;
    const uintmax_t size = fs::file_size(directory / "data", ec);
    if (ec || size != descriptor.value().size) {
        return Status(ErrorCode::kCorrupt, "chunk data size is invalid");
    }
    // A previous publish may have renamed the directory but failed to sync
    // its parent. Do not report that visible entry as committed until it is
    // durably recorded, including on an immediate retry in the same process.
    Status status = FsyncDirectory(directory.parent_path());
    if (!status.ok()) return status;
    return descriptor;
}

// 完整内容校验和
Status VerifyDataFile(const fs::path& path, const ChunkDescriptor& descriptor)
{
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return IoError("open chunk for checksum verification");
    auto checksum = Sha256File(fd, descriptor.size);
    ::close(fd);
    if (!checksum.ok()) return checksum.status();
    return checksum.value() == descriptor.checksum
               ? Status::Ok()
               : Status(ErrorCode::kChecksumMismatch, "chunk checksum does not match");
}

// 原子移动目录，确保源目录和目标目录的父目录都被同步
Status MoveDirectory(const fs::path& source, const fs::path& destination)
{
    if (::rename(source.c_str(), destination.c_str()) != 0) return IoError("rename directory");
    Status sourceSync = FsyncDirectory(source.parent_path());
    if (!sourceSync.ok()) return sourceSync;
    return FsyncDirectory(destination.parent_path());
}

fs::path FinalPath(const std::string& root, const std::string& chunkId)
{
    std::string compact;
    for (char c : chunkId) if (c != '-') compact.push_back(c);
    return fs::path(root) / "chunks" / compact.substr(0, 2) / compact.substr(2, 2) / chunkId;  // 多级目录
}

fs::path StagingPath(const std::string& root, const std::string& chunkId,
                     const std::string& uploadId, uint64_t generation)
{
    return fs::path(root) / "staging" /
           (chunkId + "." + uploadId + "." + std::to_string(generation) + ".tmp");
}

fs::path UniqueMovePath(const std::string& root, const char* area, const std::string& name)
{
    return fs::path(root) / area /
           (name + "." + std::to_string(::getpid()) + "." +
            std::to_string(gMoveSequence.fetch_add(1)));
}

}  // namespace detail

}  // namespace object_storage
