#include "storage/chunk_store/ChunkWriter.h"

#include "storage/chunk_store/ChunkStore.h"
#include "storage/chunk_store/ChunkStoreInternal.h"

#include <cerrno>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>

namespace object_storage {
namespace fs = std::filesystem;
using namespace detail;

ChunkWriter::ChunkWriter(ChunkStore* store,
                         fs::path directory,
                         std::string uploadId,
                         ChunkDescriptor descriptor,
                         int64_t expiresAtMicros,
                         int fd,
                         uint64_t receivedBytes,
                         uint64_t durableBytes)
    : store_(store),
      directory_(std::move(directory)),
      uploadId_(std::move(uploadId)),
      descriptor_(std::move(descriptor)),
      expiresAtMicros_(expiresAtMicros),
      fd_(fd),
      receivedBytes_(receivedBytes),
      durableBytes_(durableBytes)
{
}

ChunkWriter::~ChunkWriter()
{
    if (fd_ >= 0) ::close(fd_);
    ReleaseRegistration();
}

AppendResult ChunkWriter::Append(uint64_t offset, const void* data, size_t length)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (finished_) {
        return {{ErrorCode::kConflict, "writer is closed"},
                receivedBytes_, durableBytes_};
    }

    // expectedOffset 表示当前 Writer 可以继续追加的位置；durableOffset 表示
    // 进程中断后客户端能够安全恢复的位置。两者在 Checkpoint 前可能不同。
    if (offset != receivedBytes_) {
        return {{ErrorCode::kOffsetMismatch,
                 "expected offset " + std::to_string(receivedBytes_)},
                receivedBytes_, durableBytes_};
    }
    if ((length > 0 && data == nullptr) ||
        length > descriptor_.size - receivedBytes_) {
        return {{ErrorCode::kInvalidArgument, "append exceeds expected chunk size"},
                receivedBytes_, durableBytes_};
    }

    size_t written = 0;
    while (written < length) {
        const ssize_t n = ::pwrite(fd_, static_cast<const char*>(data) + written,
                                   length - written,
                                   static_cast<off_t>(receivedBytes_ + written));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            return {IoError("append chunk"), receivedBytes_, durableBytes_};
        }
        written += static_cast<size_t>(n);
    }
    receivedBytes_ += written;
    return {Status::Ok(), receivedBytes_, durableBytes_};
}

Status ChunkWriter::Checkpoint()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (finished_) return {ErrorCode::kConflict, "writer is closed"};

    // 先保证 data.part 落盘，再原子替换 upload.meta。这样恢复代码看到的
    // durable_bytes 永远不会领先于真正持久化的数据。
    if (DataSync(fd_) != 0) return IoError("sync chunk");
    UploadMetadata checkpoint{uploadId_, descriptor_, durableBytes_, expiresAtMicros_};
    checkpoint.durableBytes = receivedBytes_;
    Status status = AtomicWrite(directory_ / "upload.meta", UploadText(checkpoint));
    if (status.ok()) durableBytes_ = receivedBytes_;
    return status;
}

Result<ChunkDescriptor> ChunkWriter::Commit()
{
    std::lock_guard<std::mutex> writerLock(mutex_);
    if (committed_) return descriptor_;
    if (finished_) return Status(ErrorCode::kConflict, "writer is closed");
    if (receivedBytes_ != descriptor_.size) {
        return Status(ErrorCode::kConflict, "chunk is incomplete");
    }

    if (DataSync(fd_) != 0) return IoError("sync chunk before commit");
    auto checksum = Sha256File(fd_, descriptor_.size);
    if (!checksum.ok()) return checksum.status();
    if (!(checksum.value() == descriptor_.checksum)) {
        return Status(ErrorCode::kChecksumMismatch, "chunk checksum does not match");
    }

    // Chunk 锁只覆盖状态转换，不跟随 Writer 的整个生命周期。数据同步和哈希
    // 不需要锁住最终目录，完成校验后才串行化提交、删除等 rename 操作。
    std::lock_guard<std::mutex> chunkLock(store_->MutexFor(descriptor_.chunkId));

    // 发布顺序：同步数据 -> staging 内生成最终 data/meta -> 清除续传元数据
    // -> 同步 staging 目录 -> 原子 rename 整个目录 -> 同步最终父目录。
    ::close(fd_);
    fd_ = -1;
    if (::rename((directory_ / "data.part").c_str(), (directory_ / "data").c_str()) != 0) {
        return IoError("rename committed data");
    }
    Status status = AtomicWrite(directory_ / "meta", DescriptorText(descriptor_));
    if (!status.ok()) return status;
    if (::unlink((directory_ / "upload.meta").c_str()) != 0) {
        return IoError("remove upload metadata");
    }
    status = FsyncDirectory(directory_);
    if (!status.ok()) return status;

    const fs::path final = FinalPath(store_->root_, descriptor_.chunkId);
    status = EnsureFinalParent(store_->root_, descriptor_.chunkId);
    if (!status.ok()) return status;
    std::error_code ec;
    if (fs::exists(final, ec)) {
        auto existing = LoadCommitted(store_->root_, store_->options_.maxMetadataSize,
                                      descriptor_.chunkId);
        if (existing.ok() && existing.value() == descriptor_ &&
            VerifyDataFile(final / "data", existing.value()).ok()) {
            status = MoveDirectory(directory_, UniqueMovePath(
                store_->root_, "trash", directory_.filename().string()));
            if (!status.ok()) return status;
            finished_ = committed_ = true;
            ReleaseRegistration();
            return descriptor_;
        }
        status = MoveDirectory(directory_, UniqueMovePath(
            store_->root_, "quarantine", directory_.filename().string()));
        if (!status.ok()) return status;
        finished_ = true;
        ReleaseRegistration();
        return Status(ErrorCode::kConflict,
                      "a different committed chunk already exists");
    }
    if (::rename(directory_.c_str(), final.c_str()) != 0) return IoError("publish chunk");
    status = FsyncDirectory(final.parent_path());
    if (!status.ok()) return status;
    finished_ = committed_ = true;
    ReleaseRegistration();
    return descriptor_;
}

Status ChunkWriter::Abort()
{
    std::lock_guard<std::mutex> writerLock(mutex_);
    if (finished_) return Status::Ok();
    std::lock_guard<std::mutex> chunkLock(store_->MutexFor(descriptor_.chunkId));
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    Status status = MoveDirectory(directory_, UniqueMovePath(
        store_->root_, "trash", directory_.filename().string()));
    if (status.ok()) {
        finished_ = true;
        ReleaseRegistration();
    }
    return status;
}

uint64_t ChunkWriter::receivedBytes() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return receivedBytes_;
}

uint64_t ChunkWriter::durableBytes() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return durableBytes_;
}

void ChunkWriter::ReleaseRegistration()
{
    if (!registered_) return;
    store_->ReleaseWriter(descriptor_.chunkId, uploadId_);
    registered_ = false;
}

}  // namespace object_storage
