#ifndef OBJECT_STORAGE_COORDINATOR_CONFIG_H
#define OBJECT_STORAGE_COORDINATOR_CONFIG_H

#include <cstdint>

namespace object_storage::coordinator {

constexpr uint32_t kDefaultHeartbeatIntervalMs = 2000;
constexpr uint32_t kDefaultLeaseTimeoutMs = 10000;
constexpr uint32_t kDefaultRpcDeadlineMs = 1000;
constexpr uint32_t kDefaultConflictWaitMs = 15000;
constexpr uint32_t kMaxRpcDeadlineMs = 10000;
constexpr uint32_t kMaxConflictWaitMs = 600000;
constexpr uint32_t kMaxNodes = 1024;
constexpr uint32_t kMaxPlacementReplicas = 16;

// Compare in 64 bits so large command-line values cannot overflow.
constexpr bool ValidTiming(uint32_t heartbeatMs, uint32_t leaseMs, uint32_t rpcDeadlineMs)
{
    return heartbeatMs > 0 && rpcDeadlineMs > 0 &&
           static_cast<uint64_t>(leaseMs) >
               3ULL * heartbeatMs + rpcDeadlineMs;
}

static_assert(ValidTiming(kDefaultHeartbeatIntervalMs,
                          kDefaultLeaseTimeoutMs, kDefaultRpcDeadlineMs));

}  // namespace object_storage::coordinator

#endif
