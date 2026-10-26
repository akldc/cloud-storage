#include "storage/chunk_store/ChunkStore.h"

#include "storage/chunk_store/ChunkStoreInternal.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <sys/stat.h>
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
    errno = saved;
    return result == 0 ? Status::Ok() : IoError("fsync directory");
}

int DataSync(int fd)
{
#if defined(__linux__)
    return ::fdatasync(fd);
#else
    return ::fsync(fd);
#endif
}

Status AtomicWrite(const fs::path& path, const std::string& contents)
{
    const fs::path temporary = path.string() + ".tmp";
    const int fd = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0640);
    if (fd < 0) return IoError("open metadata temporary file");
    Status status = WriteAll(fd, contents);
    if (status.ok() && ::fsync(fd) != 0) status = IoError("fsync metadata");
    const int saved = errno;
    ::close(fd);
    errno = saved;
    if (!status.ok()) return status;
    if (::rename(temporary.c_str(), path.c_str()) != 0) return IoError("rename metadata");
    return FsyncDirectory(path.parent_path());
}

fs::path FinalPath(const std::string& root, const std::string& chunkId);

Status EnsureDirectory(const fs::path& directory)
{
    std::error_code ec;
    if (fs::exists(directory, ec)) {
        if (ec || !fs::is_directory(directory, ec)) {
            return Status(ErrorCode::kIoError, "storage path is not a directory");
        }
        return Status::Ok();
    }
    if (::mkdir(directory.c_str(), 0750) != 0) return IoError("create storage directory");
    return FsyncDirectory(directory.parent_path());
}

Status EnsureFinalParent(const std::string& root, const std::string& chunkId)
{
    const fs::path final = FinalPath(root, chunkId);
    Status first = EnsureDirectory(final.parent_path().parent_path());
    if (!first.ok()) return first;
    return EnsureDirectory(final.parent_path());
}

Status TruncateAndSync(int fd, uint64_t length)
{
    if (::ftruncate(fd, static_cast<off_t>(length)) != 0) return IoError("truncate partial chunk");
    return DataSync(fd) == 0 ? Status::Ok() : IoError("sync truncated partial chunk");
}

fs::path FinalPath(const std::string& root, const std::string& chunkId)
{
    std::string compact;
    for (char c : chunkId) if (c != '-') compact.push_back(c);
    return fs::path(root) / "chunks" / compact.substr(0, 2) / compact.substr(2, 2) / chunkId;
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
    return descriptor;
}

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

Status MoveDirectory(const fs::path& source, const fs::path& destination)
{
    if (::rename(source.c_str(), destination.c_str()) != 0) return IoError("rename directory");
    Status sourceSync = FsyncDirectory(source.parent_path());
    if (!sourceSync.ok()) return sourceSync;
    return FsyncDirectory(destination.parent_path());
}

}  // namespace detail

using namespace detail;

ChunkStore::ChunkStore(std::string root) : ChunkStore(std::move(root), Options{}) {}

ChunkStore::ChunkStore(std::string root, Options options)
    : root_(std::move(root)), options_(options) {}

ChunkStore::~ChunkStore() = default;

std::mutex& ChunkStore::MutexFor(const std::string& chunkId) const
{
    return mutexes_[std::hash<std::string>{}(chunkId) % mutexes_.size()];
}

Result<RecoveryReport> ChunkStore::Initialize()
{
    std::lock_guard<std::mutex> initializationLock(initializationMutex_);
    if (initialized_) return recoveryReport_;

    // 初始化不仅创建目录，还完成启动恢复。只有恢复成功后才对其他接口开放，
    // 避免调用方在半恢复状态下读到不完整的磁盘视图。
    std::error_code ec;
    fs::create_directories(root_, ec);
    if (ec) return Status(ErrorCode::kIoError, "create storage root: " + ec.message());
    for (const char* name : {"chunks", "staging", "trash", "quarantine"}) {
        Status status = EnsureDirectory(fs::path(root_) / name);
        if (!status.ok()) return status;
    }
    struct stat rootStat {}, childStat {};
    if (::stat(root_.c_str(), &rootStat) != 0) return IoError("stat storage root");
    for (const char* name : {"chunks", "staging", "trash", "quarantine"}) {
        if (::stat((fs::path(root_) / name).c_str(), &childStat) != 0) return IoError("stat storage area");
        if (childStat.st_dev != rootStat.st_dev) {
            return Status(ErrorCode::kInvalidArgument,
                          "storage directories must share one filesystem");
        }
    }
    auto recovery = Recover();
    if (!recovery.ok()) return recovery.status();
    recoveryReport_ = recovery.value();
    initialized_ = true;
    return recoveryReport_;
}

Status ChunkStore::RequireInitialized() const
{
    std::lock_guard<std::mutex> lock(initializationMutex_);
    return initialized_ ? Status::Ok()
                        : Status(ErrorCode::kConflict, "ChunkStore is not initialized");
}

bool ChunkStore::RegisterWriter(const std::string& chunkId, const std::string& uploadId)
{
    std::lock_guard<std::mutex> lock(activeWritersMutex_);
    const std::string attempt = chunkId + "." + uploadId;
    if (activeChunks_.count(chunkId) != 0 || activeAttempts_.count(attempt) != 0) return false;
    activeChunks_.insert(chunkId);
    activeAttempts_.insert(attempt);
    return true;
}

void ChunkStore::ReleaseWriter(const std::string& chunkId, const std::string& uploadId)
{
    std::lock_guard<std::mutex> lock(activeWritersMutex_);
    activeChunks_.erase(chunkId);
    activeAttempts_.erase(chunkId + "." + uploadId);
}

Result<std::unique_ptr<ChunkWriter>> ChunkStore::BeginWrite(const BeginChunkWrite& request)
{
    Status initialized = RequireInitialized();
    if (!initialized.ok()) return initialized;
    Status valid = ValidateDescriptor(request.descriptor, options_.maxChunkSize);
    if (!valid.ok()) return valid;
    if (!IsCanonicalId(request.uploadId) || request.expiresAtMicros < 0) {
        return Status(ErrorCode::kInvalidArgument, "upload id or expiration is invalid");
    }
    std::lock_guard<std::mutex> lock(MutexFor(request.descriptor.chunkId));
    auto existing = LoadCommitted(root_, options_.maxMetadataSize, request.descriptor.chunkId);
    if (existing.ok()) {
        if (existing.value() == request.descriptor) {
            return Status(ErrorCode::kAlreadyCommitted, "chunk is already committed");
        }
        return Status(ErrorCode::kConflict, "committed chunk descriptor differs");
    }
    if (existing.status().code() != ErrorCode::kNotFound) return existing.status();

    // Writer 不长期占用 Chunk 锁。活跃登记负责拒绝第二个 Writer，Chunk 锁只
    // 串行化目录创建、提交、删除等短期状态转换。
    if (!RegisterWriter(request.descriptor.chunkId, request.uploadId)) {
        return Status(ErrorCode::kConflict, "another writer for this chunk is active");
    }

    const fs::path directory = StagingPath(root_, request.descriptor.chunkId,
                                           request.uploadId, request.descriptor.generation);
    if (::mkdir(directory.c_str(), 0750) != 0) {
        ReleaseWriter(request.descriptor.chunkId, request.uploadId);
        return errno == EEXIST
                   ? Status(ErrorCode::kConflict, "write attempt already exists; use ResumeWrite")
                   : IoError("create staging directory");
    }
    UploadMetadata upload{request.uploadId, request.descriptor, 0, request.expiresAtMicros};
    Status status = AtomicWrite(directory / "upload.meta", UploadText(upload));
    if (!status.ok()) {
        ReleaseWriter(request.descriptor.chunkId, request.uploadId);
        return status;
    }
    const int fd = ::open((directory / "data.part").c_str(),
                          O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0640);
    if (fd < 0) {
        ReleaseWriter(request.descriptor.chunkId, request.uploadId);
        return IoError("create chunk data");
    }
    status = FsyncDirectory(directory);
    if (!status.ok()) {
        ::close(fd);
        ReleaseWriter(request.descriptor.chunkId, request.uploadId);
        return status;
    }
    return std::unique_ptr<ChunkWriter>(
        new ChunkWriter(this, directory, request.uploadId, request.descriptor,
                        request.expiresAtMicros, fd, 0, 0));
}

Result<std::unique_ptr<ChunkWriter>> ChunkStore::ResumeWrite(const ResumeChunkWrite& request)
{
    Status initialized = RequireInitialized();
    if (!initialized.ok()) return initialized;
    if (!IsCanonicalId(request.uploadId) || !IsCanonicalId(request.key.chunkId) ||
        request.key.generation == 0) {
        return Status(ErrorCode::kInvalidArgument, "resume request is invalid");
    }
    std::lock_guard<std::mutex> lock(MutexFor(request.key.chunkId));
    auto existing = LoadCommitted(root_, options_.maxMetadataSize, request.key.chunkId);
    if (existing.ok()) {
        if (existing.value().generation != request.key.generation) {
            return Status(ErrorCode::kStaleGeneration, "generation does not match");
        }
        return Status(ErrorCode::kAlreadyCommitted, "chunk is already committed");
    }
    if (existing.status().code() != ErrorCode::kNotFound) return existing.status();

    if (!RegisterWriter(request.key.chunkId, request.uploadId)) {
        return Status(ErrorCode::kConflict, "another writer for this chunk is active");
    }

    const fs::path directory = StagingPath(root_, request.key.chunkId,
                                           request.uploadId, request.key.generation);
    auto upload = ParseUpload(directory / "upload.meta", options_.maxMetadataSize);
    if (!upload.ok()) {
        ReleaseWriter(request.key.chunkId, request.uploadId);
        std::error_code ec;
        if (!fs::exists(directory, ec) && !ec) return Status(ErrorCode::kNotFound, "write not found");
        return upload.status();
    }
    if (upload.value().uploadId != request.uploadId ||
        upload.value().descriptor.chunkId != request.key.chunkId ||
        upload.value().descriptor.generation != request.key.generation) {
        ReleaseWriter(request.key.chunkId, request.uploadId);
        return Status(ErrorCode::kConflict, "resume metadata differs");
    }
    const int fd = ::open((directory / "data.part").c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) {
        ReleaseWriter(request.key.chunkId, request.uploadId);
        return IoError("open partial chunk");
    }
    struct stat statbuf {};
    if (::fstat(fd, &statbuf) != 0) {
        ::close(fd);
        ReleaseWriter(request.key.chunkId, request.uploadId);
        return IoError("stat partial chunk");
    }
    uint64_t durable = std::min<uint64_t>(upload.value().durableBytes,
                                         static_cast<uint64_t>(statbuf.st_size));
    if (static_cast<uint64_t>(statbuf.st_size) != durable) {
        Status truncated = TruncateAndSync(fd, durable);
        if (!truncated.ok()) {
            ::close(fd);
            ReleaseWriter(request.key.chunkId, request.uploadId);
            return truncated;
        }
    }
    if (upload.value().durableBytes != durable) {
        upload.value().durableBytes = durable;
        Status status = AtomicWrite(directory / "upload.meta", UploadText(upload.value()));
        if (!status.ok()) {
            ::close(fd);
            ReleaseWriter(request.key.chunkId, request.uploadId);
            return status;
        }
    }
    return std::unique_ptr<ChunkWriter>(new ChunkWriter(
        this, directory, upload.value().uploadId, upload.value().descriptor,
        upload.value().expiresAtMicros, fd, durable, durable));
}

Result<ChunkStat> ChunkStore::Stat(const ChunkKey& key) const
{
    Status initialized = RequireInitialized();
    if (!initialized.ok()) return initialized;
    if (!IsCanonicalId(key.chunkId) || key.generation == 0) {
        return Status(ErrorCode::kInvalidArgument, "chunk key is invalid");
    }
    std::lock_guard<std::mutex> lock(MutexFor(key.chunkId));
    auto descriptor = LoadCommitted(root_, options_.maxMetadataSize, key.chunkId);
    if (!descriptor.ok()) return descriptor.status();
    if (descriptor.value().generation != key.generation) {
        return Status(ErrorCode::kStaleGeneration, "generation does not match");
    }
    return ChunkStat{descriptor.value()};
}

Result<std::unique_ptr<ChunkReader>> ChunkStore::OpenRead(const ChunkKey& key) const
{
    auto stat = Stat(key);
    if (!stat.ok()) return stat.status();
    return OpenRead(key, ByteRange{0, stat.value().descriptor.size});
}

Result<std::unique_ptr<ChunkReader>> ChunkStore::OpenRead(
    const ChunkKey& key, const ByteRange& range) const
{
    Status initialized = RequireInitialized();
    if (!initialized.ok()) return initialized;
    if (!IsCanonicalId(key.chunkId) || key.generation == 0) {
        return Status(ErrorCode::kInvalidArgument, "chunk key is invalid");
    }
    std::lock_guard<std::mutex> lock(MutexFor(key.chunkId));
    auto descriptor = LoadCommitted(root_, options_.maxMetadataSize, key.chunkId);
    if (!descriptor.ok()) return descriptor.status();
    if (descriptor.value().generation != key.generation) {
        return Status(ErrorCode::kStaleGeneration, "generation does not match");
    }
    if (range.offset > descriptor.value().size) {
        return Status(ErrorCode::kInvalidArgument, "range starts past end of chunk");
    }
    if (range.length > descriptor.value().size - range.offset) {
        return Status(ErrorCode::kInvalidArgument, "range exceeds chunk size");
    }
    const int fd = ::open((FinalPath(root_, key.chunkId) / "data").c_str(),
                          O_RDONLY | O_CLOEXEC);
    if (fd < 0) return IoError("open chunk data");
    return std::unique_ptr<ChunkReader>(new ChunkReader(fd, descriptor.value(), range));
}

Status ChunkStore::Delete(const ChunkKey& key)
{
    Status initialized = RequireInitialized();
    if (!initialized.ok()) return initialized;
    if (!IsCanonicalId(key.chunkId) || key.generation == 0) {
        return {ErrorCode::kInvalidArgument, "chunk key is invalid"};
    }
    std::lock_guard<std::mutex> lock(MutexFor(key.chunkId));
    auto descriptor = LoadCommitted(root_, options_.maxMetadataSize, key.chunkId);
    if (!descriptor.ok()) {
        return descriptor.status().code() == ErrorCode::kNotFound ? Status::Ok() : descriptor.status();
    }
    if (descriptor.value().generation != key.generation) {
        return {ErrorCode::kStaleGeneration, "generation does not match"};
    }
    const fs::path final = FinalPath(root_, key.chunkId);
    return MoveDirectory(final, UniqueMovePath(root_, "trash", key.chunkId));
}

Result<VerifyResult> ChunkStore::Verify(const ChunkKey& key)
{
    Status initialized = RequireInitialized();
    if (!initialized.ok()) return initialized;
    if (!IsCanonicalId(key.chunkId) || key.generation == 0) {
        return Status(ErrorCode::kInvalidArgument, "chunk key is invalid");
    }
    std::lock_guard<std::mutex> lock(MutexFor(key.chunkId));
    auto descriptor = LoadCommitted(root_, options_.maxMetadataSize, key.chunkId);
    if (!descriptor.ok()) return descriptor.status();
    if (descriptor.value().generation != key.generation) {
        return Status(ErrorCode::kStaleGeneration, "generation does not match");
    }
    const int fd = ::open((FinalPath(root_, key.chunkId) / "data").c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return IoError("open chunk for verification");
    auto checksum = Sha256File(fd, descriptor.value().size);
    ::close(fd);
    if (!checksum.ok()) return checksum.status();
    return VerifyResult{descriptor.value(), checksum.value() == descriptor.value().checksum};
}

Result<RecoveryReport> ChunkStore::Recover()
{
    RecoveryReport report;
    std::error_code ec;

    // 最终目录只做轻量结构检查，不在启动路径扫描所有数据校验和。结构异常的
    // Chunk 会被移入 quarantine，后续普通读取无法看到它们。
    const fs::path chunks = fs::path(root_) / "chunks";
    for (fs::recursive_directory_iterator it(chunks, ec), end; it != end && !ec; it.increment(ec)) {
        if (!it->is_directory(ec)) continue;
        const fs::path directory = it->path();
        if (!fs::exists(directory / "meta", ec) && !fs::exists(directory / "data", ec)) continue;
        auto descriptor = ParseDescriptor(directory / "meta", options_.maxMetadataSize);
        bool healthy = descriptor.ok() && directory == FinalPath(root_, descriptor.value().chunkId);
        if (healthy) {
            const uintmax_t size = fs::file_size(directory / "data", ec);
            healthy = !ec && size == descriptor.value().size;
        }
        if (healthy) {
            ++report.healthyChunks;
        } else {
            it.disable_recursion_pending();
            Status moved = MoveDirectory(directory, UniqueMovePath(root_, "quarantine",
                                                                   directory.filename().string()));
            if (!moved.ok()) return moved;
            ++report.quarantined;
        }
    }
    if (ec) return Status(ErrorCode::kIoError, "scan committed chunks: " + ec.message());

    const int64_t nowMicros = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const fs::path staging = fs::path(root_) / "staging";
    for (fs::directory_iterator it(staging, ec), end; it != end && !ec; it.increment(ec)) {
        if (!it->is_directory(ec)) continue;
        const fs::path directory = it->path();
        // data + meta 且没有 upload.meta，表示进程可能中断在最终目录 rename
        // 之前。完整校验成功后可以安全完成发布，否则必须隔离。
        if (fs::exists(directory / "meta", ec) && fs::exists(directory / "data", ec) &&
            !fs::exists(directory / "upload.meta", ec)) {
            auto descriptor = ParseDescriptor(directory / "meta", options_.maxMetadataSize);
            bool complete = descriptor.ok();
            if (complete) {
                const uintmax_t size = fs::file_size(directory / "data", ec);
                complete = !ec && size == descriptor.value().size;
            }
            if (complete) {
                const int fd = ::open((directory / "data").c_str(), O_RDONLY | O_CLOEXEC);
                auto sum = fd < 0 ? Result<Checksum>(IoError("open interrupted commit"))
                                  : Sha256File(fd, descriptor.value().size);
                if (fd >= 0) ::close(fd);
                complete = sum.ok() && sum.value() == descriptor.value().checksum;
            }
            const fs::path final = complete ? FinalPath(root_, descriptor.value().chunkId) : fs::path();
            if (complete && !fs::exists(final, ec)) {
                Status parentStatus = EnsureFinalParent(root_, descriptor.value().chunkId);
                if (!parentStatus.ok()) return parentStatus;
                if (::rename(directory.c_str(), final.c_str()) != 0) return IoError("publish recovered commit");
                Status synced = FsyncDirectory(final.parent_path());
                if (!synced.ok()) return synced;
                ++report.completedCommits;
                ++report.healthyChunks;
                continue;
            }
            Status moved = MoveDirectory(directory, UniqueMovePath(root_, "quarantine",
                                                                   directory.filename().string()));
            if (!moved.ok()) return moved;
            ++report.quarantined;
            continue;
        }

        auto upload = ParseUpload(directory / "upload.meta", options_.maxMetadataSize);
        if (!upload.ok()) {
            Status moved = MoveDirectory(directory, UniqueMovePath(root_, "quarantine",
                                                                   directory.filename().string()));
            if (!moved.ok()) return moved;
            ++report.quarantined;
            continue;
        }
        // data 已改名但 upload.meta 尚未删除，是更早一步的提交中断。恢复流程
        // 重建最终 meta，再沿用与正常 Commit 相同的目录发布顺序。
        if (fs::exists(directory / "data", ec) && !fs::exists(directory / "data.part", ec)) {
            const auto& descriptor = upload.value().descriptor;
            const uintmax_t size = fs::file_size(directory / "data", ec);
            Status verified = (!ec && size == descriptor.size)
                                  ? VerifyDataFile(directory / "data", descriptor)
                                  : Status(ErrorCode::kCorrupt,
                                           "interrupted commit data size is invalid");
            if (!verified.ok()) {
                Status moved = MoveDirectory(directory, UniqueMovePath(
                    root_, "quarantine", directory.filename().string()));
                if (!moved.ok()) return moved;
                ++report.quarantined;
                ec.clear();
                continue;
            }
            Status status = AtomicWrite(directory / "meta", DescriptorText(descriptor));
            if (!status.ok()) return status;
            if (::unlink((directory / "upload.meta").c_str()) != 0 && errno != ENOENT) {
                return IoError("remove recovered upload metadata");
            }
            status = FsyncDirectory(directory);
            if (!status.ok()) return status;
            const fs::path final = FinalPath(root_, descriptor.chunkId);
            status = EnsureFinalParent(root_, descriptor.chunkId);
            if (!status.ok()) return status;
            if (fs::exists(final, ec)) {
                auto existing = LoadCommitted(root_, options_.maxMetadataSize,
                                              descriptor.chunkId);
                if (!existing.ok() || !(existing.value() == descriptor) ||
                    !VerifyDataFile(final / "data", descriptor).ok()) {
                    Status moved = MoveDirectory(directory, UniqueMovePath(
                        root_, "quarantine", directory.filename().string()));
                    if (!moved.ok()) return moved;
                    ++report.quarantined;
                    continue;
                }
                status = MoveDirectory(directory, UniqueMovePath(
                    root_, "trash", directory.filename().string()));
                if (!status.ok()) return status;
                ++report.trashed;
                continue;
            }
            if (::rename(directory.c_str(), final.c_str()) != 0) {
                return IoError("publish recovered commit");
            }
            status = FsyncDirectory(final.parent_path());
            if (!status.ok()) return status;
            ++report.completedCommits;
            ++report.healthyChunks;
            continue;
        }
        if (upload.value().expiresAtMicros != 0 && upload.value().expiresAtMicros <= nowMicros) {
            Status moved = MoveDirectory(directory, UniqueMovePath(root_, "trash",
                                                                   directory.filename().string()));
            if (!moved.ok()) return moved;
            ++report.trashed;
            continue;
        }
        const fs::path data = directory / "data.part";
        const uintmax_t actual = fs::file_size(data, ec);
        if (ec) {
            Status moved = MoveDirectory(directory, UniqueMovePath(root_, "quarantine",
                                                                   directory.filename().string()));
            if (!moved.ok()) return moved;
            ++report.quarantined;
            ec.clear();
            continue;
        }
        // 只信任 data.part 与 upload.meta 共同确认的前缀。文件更长时截断未
        // 检查点数据；文件更短时回退 durable_bytes，绝不凭空补齐数据。
        const uint64_t durable = std::min<uint64_t>(upload.value().durableBytes, actual);
        if (actual != durable) {
            const int fd = ::open(data.c_str(), O_RDWR | O_CLOEXEC);
            if (fd < 0) return IoError("open recovered partial chunk");
            Status truncated = TruncateAndSync(fd, durable);
            ::close(fd);
            if (!truncated.ok()) return truncated;
        }
        if (upload.value().durableBytes != durable) {
            upload.value().durableBytes = durable;
            Status status = AtomicWrite(directory / "upload.meta", UploadText(upload.value()));
            if (!status.ok()) return status;
        }
        ++report.resumableWrites;
    }
    if (ec) return Status(ErrorCode::kIoError, "scan staging writes: " + ec.message());
    return report;
}

}  // namespace object_storage
