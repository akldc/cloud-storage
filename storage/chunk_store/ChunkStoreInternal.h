#ifndef OBJECT_STORAGE_CHUNK_STORE_CHUNKSTOREINTERNAL_H
#define OBJECT_STORAGE_CHUNK_STORE_CHUNKSTOREINTERNAL_H

#include "storage/common/Types.h"

#include <filesystem>

namespace object_storage {
namespace detail {

struct UploadMetadata {
    std::string uploadId;
    ChunkDescriptor descriptor;
    uint64_t durableBytes{0};
    int64_t expiresAtMicros{0};
};

Status IoError(const std::string& operation);
std::string DescriptorText(const ChunkDescriptor& descriptor);
std::string UploadText(const UploadMetadata& upload);
Result<UploadMetadata> ParseUpload(const std::filesystem::path& path, size_t limit);
Status AtomicWrite(const std::filesystem::path& path, const std::string& contents);
Status FsyncDirectory(const std::filesystem::path& directory);
int DataSync(int fd);
Status EnsureFinalParent(const std::string& root, const std::string& chunkId);
std::filesystem::path FinalPath(const std::string& root, const std::string& chunkId);
std::filesystem::path StagingPath(const std::string& root,
                                  const std::string& chunkId,
                                  const std::string& uploadId,
                                  uint64_t generation);
std::filesystem::path UniqueMovePath(const std::string& root,
                                     const char* area,
                                     const std::string& name);
Status ValidateDescriptor(const ChunkDescriptor& descriptor, uint64_t maxChunkSize);
Result<ChunkDescriptor> LoadCommitted(const std::string& root,
                                      size_t metadataLimit,
                                      const std::string& chunkId);
Status VerifyDataFile(const std::filesystem::path& path,
                      const ChunkDescriptor& descriptor);
Status MoveDirectory(const std::filesystem::path& source,
                     const std::filesystem::path& destination);

}  // namespace detail
}  // namespace object_storage

#endif
