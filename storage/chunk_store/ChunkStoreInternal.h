#ifndef OBJECT_STORAGE_CHUNK_STORE_CHUNKSTOREINTERNAL_H
#define OBJECT_STORAGE_CHUNK_STORE_CHUNKSTOREINTERNAL_H

#include "storage/common/Types.h"

#include <filesystem>

namespace object_storage {
namespace detail {

// 断点续传的元数据，持久化状态，存储在 staging/<chunkId>.<uploadId>.<generation>.tmp/upload.meta
struct UploadMetadata {
    std::string uploadId;
    ChunkDescriptor descriptor;
    uint64_t durableBytes{0};  // 记录已经达到持久化保证的连续数据前缀长度
    int64_t expiresAtMicros{0};
};

Status IoError(const std::string& operation);
std::string DescriptorText(const ChunkDescriptor& descriptor);
Result<ChunkDescriptor> ParseDescriptor(const std::filesystem::path& path, size_t limit);
std::string UploadText(const UploadMetadata& upload);
Result<UploadMetadata> ParseUpload(const std::filesystem::path& path, size_t limit);

// 原子写入文件，先写入临时文件，再重命名覆盖目标文件
Status AtomicWrite(const std::filesystem::path& path, const std::string& contents);
Status FsyncDirectory(const std::filesystem::path& directory);
int DataSync(int fd);
Status EnsureDirectory(const std::filesystem::path& directory);
Status EnsureFinalParent(const std::string& root, const std::string& chunkId);
Status TruncateAndSync(int fd, uint64_t length);

std::filesystem::path FinalPath(const std::string& root, const std::string& chunkId);
std::filesystem::path StagingPath(const std::string& root, const std::string& chunkId, const std::string& uploadId, uint64_t generation);
std::filesystem::path UniqueMovePath(const std::string& root, const char* area, const std::string& name);

Status ValidateDescriptor(const ChunkDescriptor& descriptor, uint64_t maxChunkSize);
Status VerifyDataFile(const std::filesystem::path& path, const ChunkDescriptor& descriptor);
Result<ChunkDescriptor> LoadCommitted(const std::string& root, size_t metadataLimit,const std::string& chunkId);

Status MoveDirectory(const std::filesystem::path& source, const std::filesystem::path& destination);

}  // namespace detail
}  // namespace object_storage

#endif
