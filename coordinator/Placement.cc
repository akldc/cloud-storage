#include "coordinator/Placement.h"

#include "storage/common/Types.h"

#include <algorithm>
#include <map>
#include <utility>

namespace object_storage::coordinator {

// replicaCount：返回的可选节点数量
// Rendezvous Hashing（最高随机权重哈希，HRW）
Result<std::vector<NodeInfo>> GetPlacement(const std::string& chunkId,
                                            uint32_t replicaCount,
                                            const std::vector<NodeInfo>& onlineNodes)
{
    if (!IsCanonicalId(chunkId) || replicaCount == 0 || replicaCount > kMaxPlacementReplicas) {
        return Status{Code::kInvalidArgument, "invalid chunk ID or replica count"};
    }
    std::map<std::string, NodeInfo> unique;
    for (const auto& node : onlineNodes) {
        if (node.online) unique.emplace(node.nodeId, node);
    }
    if (unique.size() < replicaCount) {
        return Status{Code::kFailedPrecondition, "insufficient online nodes"};
    }

    struct Scored {
        std::string digest;
        NodeInfo info;
    };
    std::vector<Scored> scored;
    scored.reserve(unique.size());
    for (const auto& entry : unique) {
        std::string input = "coord-v1-placement";
        input.push_back('\0');
        input += chunkId;
        input.push_back('\0');
        input += entry.first;  // "coord-v1-placement" \0 chunkId \0 nodeId
        auto digest = Sha256Bytes(input.data(), input.size());
        if (!digest.ok()) return Status{Code::kInternal, digest.status().message()};
        scored.push_back({digest.value().value, entry.second});
    }
    std::sort(scored.begin(), scored.end(), [](const Scored& a, const Scored& b) {
        if (a.digest != b.digest) return a.digest > b.digest;
        return a.info.nodeId < b.info.nodeId;
    });
    std::vector<NodeInfo> result;
    result.reserve(replicaCount);
    for (uint32_t i = 0; i < replicaCount; ++i) result.push_back(std::move(scored[i].info));
    return result;
}

}  // namespace object_storage::coordinator
