#ifndef OBJECT_STORAGE_COORDINATOR_CHUNKPROBE_H
#define OBJECT_STORAGE_COORDINATOR_CHUNKPROBE_H

#include "coordinator/ObjectCatalog.h"

#include <chrono>

namespace object_storage::coordinator {

// Call only outside Catalog/Registry locks. A successful result proves that
// the target's committed HEAD descriptor matches the immutable upload plan.
Status ProbeChunk(const NodeInfo& node, const UploadRecord& upload,
                  const ChunkPlan& chunk, std::chrono::milliseconds timeout);

}  // namespace object_storage::coordinator

#endif
