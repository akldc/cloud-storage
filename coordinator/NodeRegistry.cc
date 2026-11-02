#include "coordinator/NodeRegistry.h"

#include "storage/common/Types.h"

#include <algorithm>
#include <arpa/inet.h>
#include <stdexcept>

namespace object_storage::coordinator {

NodeRegistry::NodeRegistry(std::chrono::milliseconds lease) : lease_(lease)
{
    if (lease_.count() <= 0) throw std::invalid_argument("lease must be positive");
}

bool NodeRegistry::IsOnline(const Record& record, Clock::time_point now) const
{
    return !record.unregistered && now - record.lastSeen < lease_;
}

// 返回节点的快照信息
NodeInfo NodeRegistry::Snapshot(const Record& record, Clock::time_point now) const
{
    NodeInfo info = record.info;
    info.online = IsOnline(record, now);  // 查询时才判断节点是否在线，避免频繁修改
    return info;
}

Status NodeRegistry::Register(const std::string& nodeId, const std::string& instanceId,
                              const std::string& host, uint32_t port, Clock::time_point now)
{
    if (!ValidIds(nodeId, instanceId)) {
        return {Code::kInvalidArgument, "node and instance IDs must be canonical UUIDs"};
    }
    if (port == 0 || port > 65535) {
        return {Code::kInvalidArgument, "advertised port must be 1..65535"};
    }
    auto normalized = NormalizeHost(host);
    if (!normalized.ok()) return normalized.status;

    std::lock_guard<std::mutex> lock(mutex_);
    auto existing = nodes_.find(nodeId);
    if (existing != nodes_.end()) {
        const Record& old = existing->second;
        if (old.info.instanceId == instanceId) {
            if (old.info.host != *normalized.value || old.info.port != port) {
                return {Code::kFailedPrecondition, "same instance changed endpoint"};
            }
        } else if (IsOnline(old, now)) { 
            return {Code::kAlreadyExists, "node ID held by an online instance"};  // 不允许相同node ID的新实例立即抢占在线节点
        }
    }
    // 检查 Endpoint 是否被其他在线节点占用
    for (const auto& entry : nodes_) {
        const Record& other = entry.second;
        if (entry.first != nodeId && IsOnline(other, now) &&
            other.info.host == *normalized.value && other.info.port == port) {
            return {Code::kAlreadyExists, "endpoint held by another online node"};
        }
    }

    // 如果节点注册时，节点数已经达到上限，则尝试删除一个离线节点
    if (existing == nodes_.end() && nodes_.size() >= kMaxNodes) {
        auto candidate = nodes_.end();
        for (auto it = nodes_.begin(); it != nodes_.end(); ++it) {
            if (IsOnline(it->second, now)) continue;
            if (candidate == nodes_.end() ||
                (it->second.unregistered && !candidate->second.unregistered) ||
                (it->second.unregistered == candidate->second.unregistered &&
                 (it->second.lastSeen < candidate->second.lastSeen ||
                  (it->second.lastSeen == candidate->second.lastSeen &&
                   it->first < candidate->first)))) {
                candidate = it;
            }
        }
        if (candidate == nodes_.end()) {
            return {Code::kResourceExhausted, "registry is full of online nodes"};
        }
        nodes_.erase(candidate);
    }

    // Concurrent RPCs can reach the mutex out of order after sampling `now`.
    if (existing != nodes_.end() && existing->second.info.instanceId == instanceId) {
        now = std::max(now, existing->second.lastSeen);
    }
    Record updated{{nodeId, instanceId, *normalized.value,
                    static_cast<uint16_t>(port), true, UnixMillis()}, now, false};
    nodes_[nodeId] = std::move(updated);
    return {};
}

Status NodeRegistry::Heartbeat(const std::string& nodeId, const std::string& instanceId,
                               Clock::time_point now)
{
    if (!ValidIds(nodeId, instanceId)) {
        return {Code::kInvalidArgument, "node and instance IDs must be canonical UUIDs"};
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = nodes_.find(nodeId);
    if (it == nodes_.end()) return {Code::kNotFound, "node not found"};
    Record& record = it->second;
    if (record.info.instanceId != instanceId) {
        return {Code::kFailedPrecondition, "instance ID mismatch"};  // 旧实例拒绝
    }
    if (!IsOnline(record, now)) return {Code::kNotFound, "node is offline"}; // 离线节点拒绝心跳，必须重新register

    // 更新
    record.lastSeen = std::max(now, record.lastSeen);  // 防止并发心跳 RPC 乱序导致 lastSeen 回退
    record.info.lastSeenUnixMs = UnixMillis();
    return {};
}

Status NodeRegistry::Unregister(const std::string& nodeId, const std::string& instanceId)
{
    if (!ValidIds(nodeId, instanceId)) {
        return {Code::kInvalidArgument, "node and instance IDs must be canonical UUIDs"};
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = nodes_.find(nodeId);
    if (it == nodes_.end()) return {Code::kNotFound, "node not found"};
    if (it->second.info.instanceId != instanceId) {
        return {Code::kFailedPrecondition, "instance ID mismatch"};
    }
    it->second.unregistered = true; // 没有从map中删除节点，而是标记为已注销
    return {};
}

Result<NodeInfo> NodeRegistry::GetNode(const std::string& nodeId, Clock::time_point now) const
{
    if (!IsCanonicalId(nodeId)) return Status{Code::kInvalidArgument, "invalid node ID"};
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = nodes_.find(nodeId);
    if (it == nodes_.end()) return Status{Code::kNotFound, "node not found"};
    return Snapshot(it->second, now);
}

std::vector<NodeInfo> NodeRegistry::ListNodes(Clock::time_point now) const
{
    std::vector<NodeInfo> nodes;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        nodes.reserve(nodes_.size());
        for (const auto& entry : nodes_) nodes.push_back(Snapshot(entry.second, now));
    }
    std::sort(nodes.begin(), nodes.end(),
              [](const NodeInfo& a, const NodeInfo& b) { return a.nodeId < b.nodeId; });
    return nodes;
}

std::vector<NodeInfo> NodeRegistry::OnlineNodes(Clock::time_point now) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<NodeInfo> nodes;
    for (const auto& entry : nodes_) {
        if (IsOnline(entry.second, now)) nodes.push_back(Snapshot(entry.second, now));
    }
    return nodes;
}

namespace {

Result<std::string> NormalizeHost(const std::string& host)
{
    if (host.empty() || host.size() > 253) {
        return Status{Code::kInvalidArgument, "host length must be 1..253"};
    }
    char canonical[INET6_ADDRSTRLEN];
    in_addr ipv4{};
    if (::inet_pton(AF_INET, host.c_str(), &ipv4) == 1) {
        if (ipv4.s_addr == INADDR_ANY) {
            return Status{Code::kInvalidArgument, "unspecified host is not advertised"};
        }
        ::inet_ntop(AF_INET, &ipv4, canonical, sizeof canonical);
        return std::string(canonical);
    }
    in6_addr ipv6{};
    if (::inet_pton(AF_INET6, host.c_str(), &ipv6) == 1) {
        if (IN6_IS_ADDR_UNSPECIFIED(&ipv6)) {
            return Status{Code::kInvalidArgument, "unspecified host is not advertised"};
        }
        ::inet_ntop(AF_INET6, &ipv6, canonical, sizeof canonical);
        return std::string(canonical);
    }

    // A DNS name is stored in lowercase. Reject ambiguous punctuation and
    // empty labels so malformed IP literals cannot slip through as names.
    std::string normalized;
    normalized.reserve(host.size());
    size_t labelStart = 0;
    for (size_t i = 0; i <= host.size(); ++i) {
        if (i == host.size() || host[i] == '.') {
            const size_t length = i - labelStart;
            if (length == 0 || length > 63 || host[labelStart] == '-' || host[i - 1] == '-') {
                return Status{Code::kInvalidArgument, "invalid DNS host label"};
            }
            if (i != host.size()) normalized.push_back('.');
            labelStart = i + 1;
        } else {
            const char ch = host[i];
            if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                  (ch >= '0' && ch <= '9') || ch == '-')) {
                return Status{Code::kInvalidArgument, "invalid host character"};
            }
            normalized.push_back(ch >= 'A' && ch <= 'Z' ? static_cast<char>(ch + ('a' - 'A')) : ch);
        }
    }
    return normalized;
}

bool ValidIds(const std::string& nodeId, const std::string& instanceId)
{
    return IsCanonicalId(nodeId) && IsCanonicalId(instanceId);
}

int64_t UnixMillis()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace

}  // namespace object_storage::coordinator
