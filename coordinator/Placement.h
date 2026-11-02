#ifndef OBJECT_STORAGE_COORDINATOR_PLACEMENT_H
#define OBJECT_STORAGE_COORDINATOR_PLACEMENT_H

#include "coordinator/NodeRegistry.h"

namespace object_storage::coordinator {

// Returns candidates from one online registry snapshot; it does not track data.
Result<std::vector<NodeInfo>> GetPlacement(const std::string& chunkId,
                                            uint32_t replicaCount,
                                            const std::vector<NodeInfo>& onlineNodes);

}  // namespace object_storage::coordinator

#endif
