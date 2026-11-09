#ifndef OBJECT_STORAGE_COORDINATOR_OBJECTCATALOG_H
#define OBJECT_STORAGE_COORDINATOR_OBJECTCATALOG_H

#include "coordinator/NodeRegistry.h"

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

struct sqlite3;

namespace object_storage::coordinator {

constexpr uint64_t kObjectChunkSize = 64ULL * 1024 * 1024;
constexpr uint32_t kMaxObjectChunks = 1024;
constexpr uint64_t kMaxObjectSize = kObjectChunkSize * kMaxObjectChunks;

struct ChunkInput {
    uint32_t index{0};
    uint64_t offset{0};
    uint64_t size{0};
    std::string sha256;
};

struct BeginUploadInput {
    std::string requestId;
    uint64_t totalSize{0};
    std::string fileSha256;
    std::vector<ChunkInput> chunks;
};

enum class UploadState { kPreparing, kCommitted, kAborted };

struct ChunkPlan {
    ChunkInput input;
    std::string chunkId;
    std::string targetNodeId;
    std::string confirmedNodeId;
};

struct UploadRecord {
    std::string requestId;
    std::string objectId;
    std::string uploadId;
    UploadState state{UploadState::kPreparing};
    uint64_t totalSize{0};
    std::string fileSha256;
    std::vector<ChunkPlan> chunks;
};

// One SQLite connection and one mutex. No network or hashing occurs while held.
class ObjectCatalog {
public:
    explicit ObjectCatalog(std::string dbPath);
    ~ObjectCatalog();
    ObjectCatalog(const ObjectCatalog&) = delete;
    ObjectCatalog& operator=(const ObjectCatalog&) = delete;

    Result<UploadRecord> BeginUpload(const BeginUploadInput& input,
                                     const std::vector<NodeInfo>& onlineNodes);
    Result<UploadRecord> GetUpload(const std::string& requestId);
    Result<UploadRecord> GetUploadById(const std::string& uploadId);
    Result<UploadRecord> GetObject(const std::string& objectId);
    Result<std::string> CommitUpload(const std::string& uploadId);

    // Stage B calls this only after a successful descriptor-matching HTTP HEAD.
    Status ConfirmLocation(const std::string& uploadId, uint32_t chunkIndex,
                           const std::string& nodeId);

private:
    Status SqlError(int code) const;
    Status Exec(const char* sql);
    Result<UploadRecord> Load(const char* column, const std::string& id);
    static Status Validate(const BeginUploadInput& input);
    static bool SameManifest(const UploadRecord& saved, const BeginUploadInput& input);

    std::string dbPath_;
    int lockFd_{-1};
    sqlite3* db_{nullptr};
    std::mutex mutex_;
};

}  // namespace object_storage::coordinator

#endif
