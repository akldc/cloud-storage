#ifndef OBJECT_STORAGE_NODE_STORAGENODEHTTP_H
#define OBJECT_STORAGE_NODE_STORAGENODEHTTP_H

#include "storage/chunk_store/ChunkStore.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace mymuduo { namespace net { class HttpRequest; } }

namespace object_storage {
namespace node_detail {

bool ParseUint(const std::string& text, uint64_t* value);
int HttpCode(const Status& status);
const char* Reason(int code);
std::vector<std::pair<std::string, std::string>> DescriptorHeaders(
    const ChunkDescriptor& descriptor);
bool ParseRange(const std::string& header, uint64_t size, ByteRange* range);
bool ParseUploadRequest(const mymuduo::net::HttpRequest& http,
                        const std::string& chunkId, BeginChunkWrite* request,
                        uint64_t* offset);

}  // namespace node_detail
}  // namespace object_storage

#endif
