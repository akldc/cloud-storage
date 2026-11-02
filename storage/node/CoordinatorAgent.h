#ifndef OBJECT_STORAGE_NODE_COORDINATORAGENT_H
#define OBJECT_STORAGE_NODE_COORDINATORAGENT_H

#include "coordinator/Config.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

namespace object_storage {

// Process-level control worker. It owns its gRPC stub and never accesses the
// StorageNode, EventLoop, HTTP sessions, or ChunkStore.
class CoordinatorAgent {
public:
    struct Options {
        std::string address;  // Coordinator server address, e.g. "localhost:50051"
        std::string nodeId;
        std::string advertisedHost; // StorageNode 对外广告的 host，必须是可解析的 IP 或合法的 DNS 名称
        uint16_t advertisedPort{0};
        uint32_t rpcDeadlineMs{coordinator::kDefaultRpcDeadlineMs}; // gRPC 调用的超时时间
        uint32_t conflictWaitMs{coordinator::kDefaultConflictWaitMs};  // 注册遇到节点身份冲突时最多等待时间
    };

    enum class State { kStarting, kRegistered, kRetrying, kTerminalConflict,
                       kTerminalError, kStopped };

    explicit CoordinatorAgent(Options options);
    ~CoordinatorAgent();
    CoordinatorAgent(const CoordinatorAgent&) = delete;
    CoordinatorAgent& operator=(const CoordinatorAgent&) = delete;

    void Start();
    void Stop();
    State state() const;
    std::string lastError() const;
    const std::string& instanceId() const { return instanceId_; }

private:
    void Run();
    bool WaitFor(std::chrono::milliseconds delay);
    bool Stopping() const;
    void SetState(State state, std::string error = {});

    const Options options_;
    const std::string instanceId_;
    mutable std::mutex mutex_;
    std::condition_variable stopped_;
    bool stopping_{false};
    bool started_{false};
    State state_{State::kStarting};
    std::string lastError_;
    std::thread worker_;  // Worker thread that runs the registration and heartbeat loop.
};

}  // namespace object_storage

#endif
