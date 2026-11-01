#include "storage/node/StorageNodeHttp.h"

#include "net/HttpRequest.h"

#include <algorithm>
#include <charconv>
#include <limits>

namespace object_storage {
namespace node_detail {

using Header = std::pair<std::string, std::string>;
using Headers = std::vector<Header>;

bool ParseUint(const std::string& text, uint64_t* value)
{
    if (text.empty()) return false;
    auto result = std::from_chars(text.data(), text.data() + text.size(), *value);
    return result.ec == std::errc() && result.ptr == text.data() + text.size();
}

int HttpCode(const Status& status)
{
    switch (status.code()) {
    case ErrorCode::kNotFound: return 404;
    case ErrorCode::kAlreadyCommitted: return 200;
    case ErrorCode::kConflict:
    case ErrorCode::kWriteAttemptExists:
    case ErrorCode::kOffsetMismatch:
    case ErrorCode::kStaleGeneration: return 409;
    case ErrorCode::kChecksumMismatch: return 422;
    case ErrorCode::kInvalidArgument: return 400;
    case ErrorCode::kNoSpace: return 503;
    default: return 500;
    }
}

const char* Reason(int code)
{
    switch (code) {
    case 200: return "OK";
    case 201: return "Created";
    case 202: return "Accepted";
    case 204: return "No Content";
    case 206: return "Partial Content";
    case 308: return "Resume Incomplete";
    case 400: return "Bad Request";
    case 404: return "Not Found";
    case 409: return "Conflict";
    case 416: return "Range Not Satisfiable";
    case 422: return "Unprocessable Entity";
    case 500: return "Internal Server Error";
    case 503: return "Service Unavailable";
    default: return "Error";
    }
}

Headers DescriptorHeaders(const ChunkDescriptor& descriptor)
{
    return {{"X-Generation", std::to_string(descriptor.generation)},
            {"X-Chunk-Size", std::to_string(descriptor.size)},
            {"X-Checksum-Sha256", descriptor.checksum.value}};
}

bool ParseRange(const std::string& header, uint64_t size, ByteRange* range)
{
    if (header.empty()) {
        *range = {0, size};
        return true;
    }
    if (header.compare(0, 6, "bytes=") != 0 || size == 0) return false;
    const std::string value = header.substr(6);
    const size_t dash = value.find('-');
    if (dash == std::string::npos || value.find(',', dash) != std::string::npos) return false;
    const std::string first = value.substr(0, dash);
    const std::string last = value.substr(dash + 1);
    uint64_t start = 0, end = 0;
    if (first.empty()) {
        uint64_t suffix = 0;
        if (!ParseUint(last, &suffix) || suffix == 0) return false;
        start = size - std::min(size, suffix);
        end = size - 1;
    } else {
        if (!ParseUint(first, &start) || start >= size) return false;
        if (last.empty()) end = size - 1;
        else if (!ParseUint(last, &end) || end < start) return false;
        else end = std::min(end, size - 1);
    }
    *range = {start, end - start + 1};
    return true;
}

bool ParseUploadRequest(const mymuduo::net::HttpRequest& http,
                        const std::string& chunkId, BeginChunkWrite* request,
                        uint64_t* offset)
{
    uint64_t version = 0, index = 0, logical = 0, size = 0, generation = 0;
    uint64_t expires = 0;
    const std::string expiry = http.getHeader("X-Expires-At-Micros");
    if (!ParseUint(http.getHeader("X-Object-Version"), &version) ||
        !ParseUint(http.getHeader("X-Chunk-Index"), &index) || index > UINT32_MAX ||
        !ParseUint(http.getHeader("X-Logical-Offset"), &logical) ||
        !ParseUint(http.getHeader("X-Chunk-Size"), &size) ||
        !ParseUint(http.getHeader("X-Generation"), &generation) || generation == 0 ||
        !ParseUint(http.getHeader("X-Offset"), offset) ||
        (!expiry.empty() && (!ParseUint(expiry, &expires) || expires > INT64_MAX)) ||
        http.getHeader("Content-Length").empty()) return false;
    request->uploadId = http.getHeader("X-Upload-Id");
    request->descriptor = {chunkId, http.getHeader("X-Object-Id"), version,
                           static_cast<uint32_t>(index), logical, size,
                           {ChecksumAlgorithm::kSha256, http.getHeader("X-Checksum-Sha256")},
                           generation, 1};
    request->expiresAtMicros = static_cast<int64_t>(expires);
    return IsCanonicalId(request->uploadId) &&
           IsCanonicalId(request->descriptor.objectId) &&
           ValidateChecksum(request->descriptor.checksum).ok() &&
           *offset <= size && http.getHeader("Transfer-Encoding").empty();
}


}  // namespace node_detail
}  // namespace object_storage
