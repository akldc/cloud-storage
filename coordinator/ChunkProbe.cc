#include "coordinator/ChunkProbe.h"

#include <curl/curl.h>

#include <algorithm>
#include <charconv>
#include <cctype>
#include <map>
#include <mutex>
#include <string>

namespace object_storage::coordinator {
namespace {

struct HeaderCapture {
    std::map<std::string, std::string> fields;
    size_t bytes{0};
    bool malformed{false};
};

size_t CaptureHeader(char* data, size_t size, size_t count, void* opaque)
{
    auto* capture = static_cast<HeaderCapture*>(opaque);
    if (size != 0 && count > SIZE_MAX / size) return 0;
    const size_t length = size * count;
    if (length > 16 * 1024 - capture->bytes) {
        capture->malformed = true;
        return 0;
    }
    capture->bytes += length;
    std::string line(data, length);
    if (line.compare(0, 5, "HTTP/") == 0) {
        capture->fields.clear();
        return length;
    }
    const size_t colon = line.find(':');
    if (colon == std::string::npos) return length;
    std::string name = line.substr(0, colon);
    std::transform(name.begin(), name.end(), name.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    if (name.compare(0, 2, "x-") != 0) return length;
    std::string value = line.substr(colon + 1);
    const auto first = value.find_first_not_of(" \t");
    const auto last = value.find_last_not_of(" \t\r\n");
    if (first == std::string::npos || last == std::string::npos || last < first ||
        !capture->fields.emplace(name, value.substr(first, last - first + 1)).second) {
        capture->malformed = true;
    }
    return length;
}

bool Number(const std::map<std::string, std::string>& fields, const char* key, uint64_t expected)
{
    auto it = fields.find(key);
    if (it == fields.end() || it->second.empty()) return false;
    uint64_t parsed = 0;
    const auto result = std::from_chars(it->second.data(),
                                        it->second.data() + it->second.size(), parsed);
    return result.ec == std::errc{} &&
           result.ptr == it->second.data() + it->second.size() && parsed == expected;
}

bool Text(const std::map<std::string, std::string>& fields, const char* key,
          const std::string& expected)
{
    auto it = fields.find(key);
    return it != fields.end() && it->second == expected;
}

}  // namespace

Status ProbeChunk(const NodeInfo& node, const UploadRecord& upload,
                  const ChunkPlan& chunk, std::chrono::milliseconds timeout)
{
    if (timeout.count() <= 0 || node.port == 0 || node.host.empty()) {
        return {Code::kUnavailable, "invalid or expired Chunk probe endpoint"};
    }
    static std::once_flag curlOnce;
    static CURLcode initCode = CURLE_FAILED_INIT;
    std::call_once(curlOnce, [] { initCode = curl_global_init(CURL_GLOBAL_DEFAULT); });
    if (initCode != CURLE_OK) return {Code::kUnavailable, "libcurl initialization failed"};

    CURL* curl = curl_easy_init();
    if (!curl) return {Code::kUnavailable, "cannot initialize Chunk HEAD"};
    const std::string host = node.host.find(':') == std::string::npos ? node.host :
                             "[" + node.host + "]";
    const std::string url = "http://" + host + ":" + std::to_string(node.port) +
                            "/v1/chunks/" + chunk.chunkId;
    const std::string generation = "X-Generation: 1";
    curl_slist* headers = curl_slist_append(nullptr, generation.c_str());
    if (!headers) {
        curl_easy_cleanup(curl);
        return {Code::kUnavailable, "cannot allocate Chunk HEAD headers"};
    }
    HeaderCapture captured;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(curl, CURLOPT_PROXY, "");
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, static_cast<long>(timeout.count()));
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, static_cast<long>(timeout.count()));
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, CaptureHeader);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &captured);
    const CURLcode result = curl_easy_perform(curl);
    long code = 0;
    if (result == CURLE_OK) curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (captured.malformed) return {Code::kFailedPrecondition, "malformed Chunk HEAD headers"};
    if (result != CURLE_OK) {
        return {Code::kUnavailable, std::string("Chunk HEAD failed: ") + curl_easy_strerror(result)};
    }
    if (code == 503) return {Code::kUnavailable, "target Storage Node is unavailable"};
    if (code != 200) return {Code::kFailedPrecondition, "Chunk HEAD did not return committed 200"};
    const auto& fields = captured.fields;
    if (!Text(fields, "x-chunk-id", chunk.chunkId) ||
        !Text(fields, "x-object-id", upload.objectId) ||
        !Number(fields, "x-object-version", 1) ||
        !Number(fields, "x-chunk-index", chunk.input.index) ||
        !Number(fields, "x-logical-offset", chunk.input.offset) ||
        !Number(fields, "x-chunk-size", chunk.input.size) ||
        !Text(fields, "x-checksum-sha256", chunk.input.sha256) ||
        !Number(fields, "x-generation", 1)) {
        return {Code::kFailedPrecondition, "Chunk HEAD descriptor differs from upload plan"};
    }
    return {};
}

}  // namespace object_storage::coordinator
