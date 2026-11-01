#ifndef OBJECT_STORAGE_CHUNK_STORE_CHUNKSTORE_H
#define OBJECT_STORAGE_CHUNK_STORE_CHUNKSTORE_H

#include "storage/chunk_store/ChunkReader.h"
#include "storage/chunk_store/ChunkWriter.h"

#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>

namespace object_storage {

// 一次新的上传
struct BeginChunkWrite {
    std::string uploadId;  // 上传会话标识
    ChunkDescriptor descriptor;
    int64_t expiresAtMicros{0}; // 上传会话过期时间
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
    size_t resumableWrites{0};  // 可继续上传的写入任务数量
    size_t completedCommits{0}; // 恢复过程中成功完成的提交数量
    size_t quarantined{0}; // 隔离的异常数量
    size_t trashed{0}; // 移入垃圾箱的数量
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
    // 创建目录，执行启动恢复，重复调用安全
    // 返回恢复结果
    Result<RecoveryReport> Initialize();
    // At most one live writer is allowed for the same chunk/upload attempt.
    Result<std::unique_ptr<ChunkWriter>> BeginWrite(const BeginChunkWrite& request);   // 创建新的upload任务
    Result<std::unique_ptr<ChunkWriter>> ResumeWrite(const ResumeChunkWrite& request); // 恢复未完成的upload任务
    Result<ChunkStat> Stat(const ChunkKey& key) const;
    Result<std::unique_ptr<ChunkReader>> OpenRead(const ChunkKey& key) const;
    Result<std::unique_ptr<ChunkReader>> OpenRead(const ChunkKey& key, const ByteRange& range) const;
    Status Delete(const ChunkKey& key);
    // A checksum mismatch is a successful verification with
    // checksumMatches=false. Operational errors remain outer Result failures.
    Result<VerifyResult> Verify(const ChunkKey& key);

private:
    friend class ChunkWriter;
    Result<RecoveryReport> Recover();
    Status RequireInitialized() const;  // 检查是否初始化

    // 保证同一chunk只有一个活跃writer
    bool RegisterWriter(const std::string& chunkId, const std::string& uploadId);
    void ReleaseWriter(const std::string& chunkId, const std::string& uploadId);
    std::mutex& MutexFor(const std::string& chunkId) const;

    std::string root_;  // 存储根目录
    Options options_;
    int lockFd_{-1};   // 文件锁，防止多个 ChunkStore 实例同时访问同一目录

    mutable std::array<std::mutex, 64> mutexes_; // 分段锁

    // 保护初始化状态和恢复报告
    mutable std::mutex initializationMutex_;
    bool initialized_{false};
    RecoveryReport recoveryReport_;

    // 活跃写入登记，防止同一 chunk/uploadId 的第二个写入
    std::mutex activeWritersMutex_;
    std::set<std::string> activeChunks_;
    std::set<std::string> activeAttempts_;

    std::atomic<size_t> liveWriters_{0};  // 当前存活的writer数量
};

}  // namespace object_storage

#endif
