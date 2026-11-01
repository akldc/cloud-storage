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


using namespace detail;

ChunkStore::ChunkStore(std::string root) : ChunkStore(std::move(root), Options{}) {}

ChunkStore::ChunkStore(std::string root, Options options)
    : root_(std::move(root)), options_(options) {}

ChunkStore::~ChunkStore()
{
    if (liveWriters_.load() != 0) std::terminate();
    if (lockFd_ >= 0) ::close(lockFd_);
}

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
    // Create every missing ancestor through EnsureDirectory so a newly created
    // root also has a durable directory entry before a write is acknowledged.
    std::error_code ec;
    const fs::path absoluteRoot = fs::absolute(root_, ec);
    if (ec) return Status(ErrorCode::kIoError, "resolve storage root: " + ec.message());

    // 逐级创建目录
    fs::path current;
    for (const auto& component : absoluteRoot) {
        current /= component;
        if (current == current.root_path()) continue;
        Status status = EnsureDirectory(current);
        if (!status.ok()) return status;
        status = FsyncDirectory(current.parent_path());
        if (!status.ok()) return status;
    }

    // linux flock 实现进程间互斥锁，防止多个 ChunkStore 实例同时访问同一目录
    const int fd = ::open((fs::path(root_) / ".chunk-store.lock").c_str(),
                          O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0640);
    if (fd < 0) return IoError("open storage root lock");
    // 获取独占锁，非阻塞模式，如果已经被其他进程锁定则返回 EWOULDBLOCK/EAGAIN
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        const int saved = errno;
        ::close(fd);
        if (saved == EWOULDBLOCK || saved == EAGAIN) {
            return Status(ErrorCode::kConflict, "storage root is already in use");
        }
        errno = saved;
        return IoError("lock storage root");
    }
    struct LockGuard {
        int fd;
        ~LockGuard() { if (fd >= 0) ::close(fd); }
    } guard{fd};  // 确保在异常或错误返回时释放文件描述符

    for (const char* name : {"chunks", "staging", "trash", "quarantine"}) {
        Status status = EnsureDirectory(fs::path(root_) / name);
        if (!status.ok()) return status;
        status = FsyncDirectory(fs::path(root_) / name);
        if (!status.ok()) return status;
    }

    // 检查所有存储目录是否在同一文件系统上，避免跨文件系统移动目录失败
    // 只有当源目录和目标目录在同一文件系统上时，rename() 可以提供原子的移动操作。否则，rename() 会返回 EXDEV 错误。
    struct stat rootStat {}, childStat {};
    if (::stat(root_.c_str(), &rootStat) != 0) return IoError("stat storage root");
    for (const char* name : {"chunks", "staging", "trash", "quarantine"}) {
        if (::stat((fs::path(root_) / name).c_str(), &childStat) != 0) return IoError("stat storage area");
        if (childStat.st_dev != rootStat.st_dev) {  // st_dev 表示文件系统设备号
            return Status(ErrorCode::kInvalidArgument,
                          "storage directories must share one filesystem");
        }
    }

    // 执行启动恢复，扫描 staging 目录，清理过期或损坏的上传任务，恢复未完成的上传任务。
    auto recovery = Recover();
    if (!recovery.ok()) return recovery.status();
    recoveryReport_ = recovery.value();
    lockFd_ = guard.fd;  // 保留锁文件描述符，直到 ChunkStore 析构时释放锁
    guard.fd = -1;
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

    // chunk粒度的互斥锁，保证同一chunkId的写入串行化，不同chunkId的写入可以并行
    std::lock_guard<std::mutex> lock(MutexFor(request.descriptor.chunkId));
    auto existing = LoadCommitted(root_, options_.maxMetadataSize, request.descriptor.chunkId);
    if (existing.ok()) {
        if (existing.value() == request.descriptor) {
            return Status(ErrorCode::kAlreadyCommitted, "chunk is already committed");
        }
        return Status(ErrorCode::kConflict, "committed chunk descriptor differs");
    }
    if (existing.status().code() != ErrorCode::kNotFound) return existing.status();  // 只有返回kNotFound时才创建新的上传任务

    // Writer 不长期占用 Chunk 锁
    // 活跃登记负责拒绝第二个 Writer
    if (!RegisterWriter(request.descriptor.chunkId, request.uploadId)) {
        return Status(ErrorCode::kConflict, "another writer for this chunk is active");
    }

    // 创建staging目录，存放上传任务的upload.meta和data.part
    const fs::path directory = StagingPath(root_, request.descriptor.chunkId,
                                           request.uploadId, request.descriptor.generation);
    if (::mkdir(directory.c_str(), 0750) != 0) {
        ReleaseWriter(request.descriptor.chunkId, request.uploadId);
        return errno == EEXIST
                   ? Status(ErrorCode::kWriteAttemptExists, "write attempt already exists")
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
    status = FsyncDirectory(fs::path(root_) / "staging");
    if (!status.ok()) {
        ::close(fd);
        ReleaseWriter(request.descriptor.chunkId, request.uploadId);
        return status;
    }
    try {
        return std::unique_ptr<ChunkWriter>(
            new ChunkWriter(this, directory, request.uploadId, request.descriptor,
                            request.expiresAtMicros, fd, 0, 0));
    } catch (...) {
        ::close(fd);
        ReleaseWriter(request.descriptor.chunkId, request.uploadId);
        throw;
    }
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
    // 检查续传的upload.meta是否与当前请求一致
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
    if (::fstat(fd, &statbuf) != 0) {  // fstat 获取文件描述符对应的文件状态信息
        ::close(fd);
        ReleaseWriter(request.key.chunkId, request.uploadId);
        return IoError("stat partial chunk");
    }
    uint64_t durable = std::min<uint64_t>(upload.value().durableBytes,
                                         static_cast<uint64_t>(statbuf.st_size));
    if (static_cast<uint64_t>(statbuf.st_size) != durable) {
        Status truncated = TruncateAndSync(fd, durable);  // 截断超出可信前缀的数据
        if (!truncated.ok()) {
            ::close(fd);
            ReleaseWriter(request.key.chunkId, request.uploadId);
            return truncated;
        }
    }
    if (upload.value().durableBytes != durable) { // 必要时回退 upload.meta 中的进度
        upload.value().durableBytes = durable;
        Status status = AtomicWrite(directory / "upload.meta", UploadText(upload.value()));
        if (!status.ok()) {
            ::close(fd);
            ReleaseWriter(request.key.chunkId, request.uploadId);
            return status;
        }
    }
    Status parentStatus = FsyncDirectory(fs::path(root_) / "staging");
    if (!parentStatus.ok()) {
        ::close(fd);
        ReleaseWriter(request.key.chunkId, request.uploadId);
        return parentStatus;
    }
    try {
        return std::unique_ptr<ChunkWriter>(new ChunkWriter(
            this, directory, upload.value().uploadId, upload.value().descriptor,
            upload.value().expiresAtMicros, fd, durable, durable));
    } catch (...) {
        ::close(fd);
        ReleaseWriter(request.key.chunkId, request.uploadId);
        throw;
    }
}

// 读取 Chunk 的描述信息，而不是读取数据内容
// 核心步骤——获取分片锁，调用LoadCommitted
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

Result<std::unique_ptr<ChunkReader>> ChunkStore::OpenRead(const ChunkKey& key, const ByteRange& range) const
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
    {
        std::lock_guard<std::mutex> activeLock(activeWritersMutex_);
        if (activeChunks_.count(key.chunkId) != 0) {
            return {ErrorCode::kConflict, "a writer for this chunk is active"};
        }
    }
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

// 验证完整data校验和
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
    std::set<fs::path> healthyParents;

    // 扫描 chunks 目录，检查每个 chunk 的 meta 和 data 是否完整，是否与路径一致。
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
            healthyParents.insert(directory.parent_path());
        } else {
            it.disable_recursion_pending(); // 不再递归扫描该目录下的子目录
            Status moved = MoveDirectory(directory, UniqueMovePath(root_, "quarantine",
                                                                   directory.filename().string()));
            if (!moved.ok()) return moved;
            ++report.quarantined;
        }
    }
    if (ec) return Status(ErrorCode::kIoError, "scan committed chunks: " + ec.message());
    for (const fs::path& parent : healthyParents) {
        Status status = FsyncDirectory(parent);
        if (!status.ok()) return status;
    }

    // 扫描 staging 目录，检查每个上传任务的 meta 和 data 是否完整，是否过期，是否与路径一致。
    const int64_t nowMicros = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const fs::path staging = fs::path(root_) / "staging";
    for (fs::directory_iterator it(staging, ec), end; it != end && !ec; it.increment(ec)) {
        if (!it->is_directory(ec)) continue;
        const fs::path directory = it->path();
        // 分支1：已经准备好提交的 chunk，包含data + meta 且没有 upload.meta
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
                complete = sum.ok() && sum.value() == descriptor.value().checksum;  // 验证数据完整性
            }
            const fs::path final = complete ? FinalPath(root_, descriptor.value().chunkId) : fs::path();
            if (complete && !fs::exists(final, ec)) {
                Status parentStatus = EnsureFinalParent(root_, descriptor.value().chunkId);
                if (!parentStatus.ok()) return parentStatus;
                Status synced = MoveDirectory(directory, final);
                if (!synced.ok()) return synced;
                ++report.completedCommits;
                ++report.healthyChunks;
                continue;
            }

            // 如果数据不完整或已经存在同名 chunk，则将该上传任务移动到 quarantine 目录，等待人工处理
            Status moved = MoveDirectory(directory, UniqueMovePath(root_, "quarantine",
                                                                   directory.filename().string()));
            if (!moved.ok()) return moved;
            ++report.quarantined;
            continue;
        }

        // 分支2：正在进行中的上传任务，包含 upload.meta + data.part，可能已经过期或损坏
        auto upload = ParseUpload(directory / "upload.meta", options_.maxMetadataSize);
        if (!upload.ok()) {
            Status moved = MoveDirectory(directory, UniqueMovePath(root_, "quarantine",
                                                                   directory.filename().string()));
            if (!moved.ok()) return moved;
            ++report.quarantined;
            continue;
        }
        
        // 分支3：已经完成上传但未提交的 chunk，包含 upload.meta + data，可能已经过期或损坏
        if (fs::exists(directory / "data", ec) && !fs::exists(directory / "data.part", ec)) {
            const auto& descriptor = upload.value().descriptor;
            const uintmax_t size = fs::file_size(directory / "data", ec);
            Status verified = (!ec && size == descriptor.size)
                                  ? VerifyDataFile(directory / "data", descriptor)
                                  : Status(ErrorCode::kCorrupt,
                                           "interrupted commit data size is invalid");
            if (!verified.ok()) { // 数据损坏或不完整，移动到 quarantine 目录
                Status moved = MoveDirectory(directory, UniqueMovePath(
                    root_, "quarantine", directory.filename().string()));
                if (!moved.ok()) return moved;
                ++report.quarantined;
                ec.clear();
                continue;
            }
            // 如果数据完整且与路径一致，则将该上传任务提交到 chunks 目录
            Status status = AtomicWrite(directory / "meta", DescriptorText(descriptor)); // 生成最终meta文件
            if (!status.ok()) return status;
            if (::unlink((directory / "upload.meta").c_str()) != 0 && errno != ENOENT) {
                return IoError("remove recovered upload metadata");
            }
            status = FsyncDirectory(directory);
            if (!status.ok()) return status;
            const fs::path final = FinalPath(root_, descriptor.chunkId);
            status = EnsureFinalParent(root_, descriptor.chunkId);
            if (!status.ok()) return status;
            // 如果目标路径已经存在同名 chunk，则需要检查是否与当前上传任务一致
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
            // 不存在同名 chunk，则将该上传任务移动到 chunks 目录，完成提交
            status = MoveDirectory(directory, final);
            if (!status.ok()) return status;
            ++report.completedCommits;
            ++report.healthyChunks;
            continue;
        }

        // 分支4：清理过期的上传任务，移动到 trash 目录
        if (upload.value().expiresAtMicros != 0 && upload.value().expiresAtMicros <= nowMicros) {
            Status moved = MoveDirectory(directory, UniqueMovePath(root_, "trash",
                                                                   directory.filename().string()));
            if (!moved.ok()) return moved;
            ++report.trashed;
            continue;
        }

        // 分支5：恢复未完成的上传任务，检查 data.part 的大小是否与 upload.meta 中的 durable_bytes 一致
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
        // actual, durable取两者中较小的值，防止 durable_bytes 超过实际文件大小
        const uint64_t durable = std::min<uint64_t>(upload.value().durableBytes, actual);
        if (actual != durable) {
            const int fd = ::open(data.c_str(), O_RDWR | O_CLOEXEC);
            if (fd < 0) return IoError("open recovered partial chunk");
            Status truncated = TruncateAndSync(fd, durable);  // 截断文件到 durable_bytes
            ::close(fd);
            if (!truncated.ok()) return truncated;
        }
        if (upload.value().durableBytes != durable) {
            upload.value().durableBytes = durable;
            Status status = AtomicWrite(directory / "upload.meta", UploadText(upload.value()));
            if (!status.ok()) return status;
        }
        ++report.resumableWrites;  // 可续传的上传任务
    }
    if (ec) return Status(ErrorCode::kIoError, "scan staging writes: " + ec.message());
    return report;
}

}  // namespace object_storage
