#ifndef OBJECT_STORAGE_CHUNK_STORE_CHUNKWRITER_H
#define OBJECT_STORAGE_CHUNK_STORE_CHUNKWRITER_H

#include "storage/common/Types.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>

namespace object_storage {

class ChunkStore;

struct AppendResult {
    Status status;
    uint64_t expectedOffset{0};
    uint64_t durableOffset{0};

    bool ok() const { return status.ok(); }
};

// A writer must not outlive the ChunkStore that created it.
class ChunkWriter {
public:
    ~ChunkWriter();
    ChunkWriter(const ChunkWriter&) = delete;
    ChunkWriter& operator=(const ChunkWriter&) = delete;

    AppendResult Append(uint64_t offset, const void* data, size_t length);
    Status Checkpoint();
    Result<ChunkDescriptor> Commit();
    Status Abort();
    uint64_t receivedBytes() const;
    uint64_t durableBytes() const;

private:
    friend class ChunkStore;
    ChunkWriter(ChunkStore* store,
                std::filesystem::path directory,
                std::string uploadId,
                ChunkDescriptor descriptor,
                int64_t expiresAtMicros,
                int fd,
                uint64_t receivedBytes,
                uint64_t durableBytes);
    void ReleaseRegistration();

    ChunkStore* store_;
    std::filesystem::path directory_;
    std::string uploadId_;
    ChunkDescriptor descriptor_;
    int64_t expiresAtMicros_;
    int fd_;
    uint64_t receivedBytes_;
    uint64_t durableBytes_;
    mutable std::mutex mutex_;
    bool registered_{true};
    bool finished_{false};
    bool committed_{false};
};

}  // namespace object_storage

#endif
