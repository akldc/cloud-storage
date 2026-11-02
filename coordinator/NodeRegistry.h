#ifndef OBJECT_STORAGE_COORDINATOR_NODEREGISTRY_H
#define OBJECT_STORAGE_COORDINATOR_NODEREGISTRY_H

#include "coordinator/Config.h"
#include "coordinator/Status.h"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace object_storage::coordinator {

using Clock = std::chrono::steady_clock;

struct NodeInfo {
    std::string nodeId;
    std::string instanceId;
    std::string host;
    uint16_t port{0};
    bool online{false};
    int64_t lastSeenUnixMs{0};
};

class NodeRegistry {
public:
    explicit NodeRegistry(std::chrono::milliseconds lease =
                              std::chrono::milliseconds(kDefaultLeaseTimeoutMs));

    Status Register(const std::string& nodeId, const std::string& instanceId,
                    const std::string& host, uint32_t port, Clock::time_point now);
    Status Heartbeat(const std::string& nodeId, const std::string& instanceId,
                     Clock::time_point now);
    Status Unregister(const std::string& nodeId, const std::string& instanceId);
    Result<NodeInfo> GetNode(const std::string& nodeId, Clock::time_point now) const;
    std::vector<NodeInfo> ListNodes(Clock::time_point now) const;
    std::vector<NodeInfo> OnlineNodes(Clock::time_point now) const;

private:
    struct Record {
        NodeInfo info;
        Clock::time_point lastSeen;  // 用于精确判断租约是否过期。
        bool unregistered{false}; // 标记节点是否已注销，注销的节点不再被认为是在线的。
    };

    bool IsOnline(const Record& record, Clock::time_point now) const;
    NodeInfo Snapshot(const Record& record, Clock::time_point now) const;

    const std::chrono::milliseconds lease_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Record> nodes_;  // 核心map
};

}  // namespace object_storage::coordinator

#endif
