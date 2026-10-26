#ifndef OBJECT_STORAGE_CHUNK_STORE_CHUNKSTORE_H
#define OBJECT_STORAGE_CHUNK_STORE_CHUNKSTORE_H

#include "storage/chunk_store/ChunkReader.h"
#include "storage/chunk_store/ChunkWriter.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>

namespace object_storage {

struct BeginChunkWrite {
    std::string uploadId;
    ChunkDescriptor descriptor;
    int64_t expiresAtMicros{0};
};

struct ResumeChunkWrite {
    std::string uploadId;
    ChunkKey key;
};

struct ChunkStat {
    ChunkDescriptor descriptor;
};

struct VerifyResult {
    ChunkDescriptor descriptor;
    bool checksumMatches{false};
};

struct RecoveryReport {
    size_t healthyChunks{0};
    size_t resumableWrites{0};
    size_t completedCommits{0};
    size_t quarantined{0};
    size_t trashed{0};
};

class ChunkStore {
public:
    struct Options {
        uint64_t maxChunkSize{256ULL * 1024 * 1024};
        size_t maxMetadataSize{16 * 1024};
    };

    explicit ChunkStore(std::string root);
    ChunkStore(std::string root, Options options);
    ~ChunkStore();
    ChunkStore(const ChunkStore&) = delete;
    ChunkStore& operator=(const ChunkStore&) = delete;

    // Initializes directories and performs startup recovery. Repeated calls are
    // safe and return the original recovery report. Call before any other API.
    Result<RecoveryReport> Initialize();
    // At most one live writer is allowed for the same chunk/upload attempt.
    Result<std::unique_ptr<ChunkWriter>> BeginWrite(const BeginChunkWrite& request);
    Result<std::unique_ptr<ChunkWriter>> ResumeWrite(const ResumeChunkWrite& request);
    Result<ChunkStat> Stat(const ChunkKey& key) const;
    Result<std::unique_ptr<ChunkReader>> OpenRead(const ChunkKey& key) const;
    Result<std::unique_ptr<ChunkReader>> OpenRead(const ChunkKey& key,
                                                  const ByteRange& range) const;
    Status Delete(const ChunkKey& key);
    // A checksum mismatch is a successful verification with
    // checksumMatches=false. Operational errors remain outer Result failures.
    Result<VerifyResult> Verify(const ChunkKey& key);

private:
    friend class ChunkWriter;
    Result<RecoveryReport> Recover();
    Status RequireInitialized() const;
    bool RegisterWriter(const std::string& chunkId, const std::string& uploadId);
    void ReleaseWriter(const std::string& chunkId, const std::string& uploadId);
    std::mutex& MutexFor(const std::string& chunkId) const;

    std::string root_;
    Options options_;
    mutable std::array<std::mutex, 64> mutexes_;
    mutable std::mutex initializationMutex_;
    bool initialized_{false};
    RecoveryReport recoveryReport_;
    std::mutex activeWritersMutex_;
    std::set<std::string> activeChunks_;
    std::set<std::string> activeAttempts_;
};

}  // namespace object_storage

#endif
