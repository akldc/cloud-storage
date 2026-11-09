#include "client/ObjectClientInternal.h"

#include <curl/curl.h>

#include <algorithm>
#include <charconv>
#include <cctype>
#include <map>
#include <mutex>
#include <vector>

namespace object_storage {
namespace detail {

// RAII
using Curl = std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>;
using Headers = std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)>;

bool InitCurl(std::string* error)
{
    static std::once_flag once;
    static CURLcode code = CURLE_FAILED_INIT;
    std::call_once(once, [] { code = curl_global_init(CURL_GLOBAL_DEFAULT); });  // call_once 保证libcurl全局初始化只执行一次
    if (code == CURLE_OK) return true;
    *error = "libcurl initialization failed";
    return false;
}

// 根据 StorageNode 的 host， port 和 chunkId 生成 HTTP URL
std::string Url(const proto::NodeInfo& node, const std::string& chunkId)
{
    const std::string host = node.host().find(':') == std::string::npos ? node.host() :
                             "[" + node.host() + "]";
    return "http://" + host + ":" + std::to_string(node.port()) + "/v1/chunks/" + chunkId;
}

void Configure(CURL* curl, const std::string& url)
{
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(curl, CURLOPT_PROXY, "");
    curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 3000L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 30L);
}



// 以下是核心辅助函数

/// @brief 封装上传源文件的状态
struct UploadSource {
    int fd;
    uint64_t base;   // 本次从源文件的哪个偏移量开始读取  base=chunk.offset() + resumeOffset(chunk内部已经持久化的偏移量)
    uint64_t sent{0}; // 本次put已经发送的字节数（实时状态）
    uint64_t size; // 本次put预期发送的总字节数。本次put需要从base开始发送size字节数据
    bool failed{false};
};


// libcurl 读回调函数
size_t ReadFile(char* buffer, size_t size, size_t count, void* opaque)
{
    auto* source = static_cast<UploadSource*>(opaque);
    if (size != 0 && count > SIZE_MAX / size) { source->failed = true; return CURL_READFUNC_ABORT; }
    const size_t capacity = size * count;  // libcurl当前提供的缓冲区大小
    const size_t wanted = static_cast<size_t>(std::min<uint64_t>(capacity, source->size - source->sent));
    if (wanted == 0) return 0;
    for (;;) {
        const ssize_t read = ::pread(source->fd, buffer, wanted,
                                     static_cast<off_t>(source->base + source->sent));  // pread
        if (read < 0 && errno == EINTR) continue;
        if (read <= 0) {
            source->failed = true;
            return CURL_READFUNC_ABORT;
        }
        source->sent += static_cast<uint64_t>(read);  // 读取成功，修改 sent 状态
        return static_cast<size_t>(read);
    }
}

// libcurl 写回调函数，用于丢弃 HTTP 响应体数据
size_t Discard(char*, size_t size, size_t count, void* opaque)
{
    auto* received = static_cast<size_t*>(opaque);
    if (size != 0 && count > SIZE_MAX / size) return 0;
    const size_t length = size * count;
    if (length > 4096 - std::min(*received, size_t{4096})) return 0;
    *received += length;
    return length;
}

bool AddHeader(curl_slist** headers, const std::string& value)
{
    auto* next = curl_slist_append(*headers, value.c_str());
    if (!next) return false;
    *headers = next;
    return true;
}

// libcurl回调函数，用于捕获 HTTP PUT 响应头
size_t CapturePutHeader(char* data, size_t size, size_t count, void* opaque)
{
    auto* put = static_cast<PutResult*>(opaque);
    if (size != 0 && count > SIZE_MAX / size) return 0;
    std::string line(data, size * count);
    std::transform(line.begin(), line.end(), line.begin(), [](unsigned char c) {  // 将响应头字段名转换为小写
        return static_cast<char>(std::tolower(c));
    });
    if (line.find("x-commit-state: unknown") == 0) put->uncertain = true;  // unknown
    return size * count;
}

// 从本地文件读取指定 Chunk 的剩余数据，通过 HTTP PUT 发送到指定 Storage Node
bool PutChunk(const proto::NodeInfo& node, const proto::GetUploadReply& plan,
              const proto::PlannedChunk& chunk, int fd, uint64_t resumeOffset,
              PutResult* put, std::string* error)
{
    if (!InitCurl(error)) return false;
    Curl curl(curl_easy_init(), curl_easy_cleanup);
    if (!curl) { *error = "cannot initialize HTTP PUT"; return false; }
    if (resumeOffset >= chunk.size()) { *error = "invalid PUT resume offset"; return false; }

    // headers
    Headers headers(nullptr, curl_slist_free_all);
    curl_slist* raw = nullptr;
    for (const auto& value : {"Expect:", "Connection: close"}) {
        if (!AddHeader(&raw, value)) { curl_slist_free_all(raw); *error = "HTTP header allocation failed"; return false; }
    }
    for (const auto& value : std::vector<std::string>{
             "X-Upload-Id: " + plan.upload_id(),
             "X-Object-Id: " + plan.object_id(),
             "X-Object-Version: 1", "X-Chunk-Index: " + std::to_string(chunk.index()),
             "X-Logical-Offset: " + std::to_string(chunk.offset()),  // chunk在整个对象中的逻辑偏移量
             "X-Chunk-Size: " + std::to_string(chunk.size()),
             "X-Checksum-Sha256: " + chunk.sha256(), "X-Generation: 1",
             "X-Offset: " + std::to_string(resumeOffset)}) { // 本次上传从 该chunk 的哪个偏移量开始
        if (!AddHeader(&raw, value)) { curl_slist_free_all(raw); *error = "HTTP header allocation failed"; return false; }
    }
    headers.reset(raw);
    const std::string url = Url(node, chunk.chunk_id());
    Configure(curl.get(), url);

    // 上传数据上下文，用于读回调
    UploadSource source{fd, chunk.offset() + resumeOffset, 0, chunk.size() - resumeOffset, false};
    size_t responseBytes = 0;
    // curl是unique_ptr，get返回原始指针 CURL*
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
    curl_easy_setopt(curl.get(), CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl.get(), CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(source.size));
    curl_easy_setopt(curl.get(), CURLOPT_READFUNCTION, ReadFile); // 设置读回调函数，从本地文件读取数据
    curl_easy_setopt(curl.get(), CURLOPT_READDATA, &source);  // 将source的地址传递给读回调函数
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, Discard);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &responseBytes);
    curl_easy_setopt(curl.get(), CURLOPT_HEADERFUNCTION, CapturePutHeader); // 处理响应Header
    curl_easy_setopt(curl.get(), CURLOPT_HEADERDATA, put);
    const CURLcode result = curl_easy_perform(curl.get());
    long code = 0;
    if (result == CURLE_OK) curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &code);
    if (result != CURLE_OK || source.failed || source.sent != source.size) {  // 检查source.sent==source.size，确保所有数据都已上传;否则上传失败
        put->sourceFailed = source.failed;
        *error = source.failed ? "source file changed or became unreadable during PUT" :
                 std::string("HTTP PUT failed: ") + curl_easy_strerror(result);
        return false;
    }
    put->code = code;
    return true;
}

struct ReplyHeaders {
    std::map<std::string, std::string> fields;
    size_t bytes{0};
    bool bad{false};
};

size_t CaptureHeader(char* data, size_t size, size_t count, void* opaque)
{
    auto* capture = static_cast<ReplyHeaders*>(opaque);
    if (size != 0 && count > SIZE_MAX / size) { capture->bad = true; return 0; }
    const size_t length = size * count;
    if (length > 16 * 1024 - capture->bytes) { capture->bad = true; return 0; }
    capture->bytes += length;
    std::string line(data, length);
    if (line.compare(0, 5, "HTTP/") == 0) { capture->fields.clear(); return length; }
    const size_t colon = line.find(':');
    if (colon == std::string::npos) return length;
    std::string name = line.substr(0, colon);
    std::transform(name.begin(), name.end(), name.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (name.compare(0, 2, "x-") != 0 && name != "content-length") return length;
    std::string value = line.substr(colon + 1);
    const auto first = value.find_first_not_of(" \t");
    const auto last = value.find_last_not_of(" \t\r\n");
    if (first == std::string::npos || last == std::string::npos || last < first ||
        !capture->fields.emplace(name, value.substr(first, last - first + 1)).second) {
        capture->bad = true;
    }
    return length;
}

bool Number(const ReplyHeaders& headers, const char* name, uint64_t expected)
{
    const auto it = headers.fields.find(name);
    if (it == headers.fields.end() || it->second.empty()) return false;
    uint64_t parsed = 0;
    const auto result = std::from_chars(it->second.data(), it->second.data() + it->second.size(), parsed);
    return result.ec == std::errc{} && result.ptr == it->second.data() + it->second.size() &&
           parsed == expected;
}

bool ReadNumber(const ReplyHeaders& headers, const char* name, uint64_t* value)
{
    const auto it = headers.fields.find(name);
    if (it == headers.fields.end() || it->second.empty()) return false;
    const auto parsed = std::from_chars(it->second.data(),
                                        it->second.data() + it->second.size(), *value);
    return parsed.ec == std::errc{} && parsed.ptr == it->second.data() + it->second.size();
}

bool Text(const ReplyHeaders& headers, const char* name, const std::string& expected)
{
    const auto it = headers.fields.find(name);
    return it != headers.fields.end() && it->second == expected;
}

struct DownloadSink {
    int fd;
    uint64_t base;  // chunk在整个输出文件中的逻辑偏移量
    uint64_t received{0}; // 本次GET已经接收的字节数（实时状态）
    uint64_t expected; // 当前chunk预期下载的总字节数
    EVP_MD_CTX* chunkDigest; // 当前chunk的SHA-256上下文
    EVP_MD_CTX* fileDigest; // 整个文件的SHA-256上下文
    bool failed{false};
};

// libcurl 写回调函数，用于将 HTTP 响应体数据写入本地文件，并更新 SHA-256 摘要
size_t WriteFile(char* data, size_t size, size_t count, void* opaque)
{
    auto* sink = static_cast<DownloadSink*>(opaque);
    if (size != 0 && count > SIZE_MAX / size) { sink->failed = true; return 0; }
    const size_t length = size * count;  // 本次libcurl交给回调函数的数据长度
    if (length > sink->expected - sink->received) { sink->failed = true; return 0; }

    size_t done = 0;
    while (done < length) {
        const ssize_t written = ::pwrite(sink->fd, data + done, length - done,
                                         static_cast<off_t>(sink->base + sink->received + done));
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) { sink->failed = true; return 0; }
        done += static_cast<size_t>(written);
    }
    // 更新两个摘要
    if (EVP_DigestUpdate(sink->chunkDigest, data, length) != 1 ||
        EVP_DigestUpdate(sink->fileDigest, data, length) != 1) {
        sink->failed = true;
        return 0;
    }
    sink->received += length;  // 更新received状态
    return length;
}

// 通过 HTTP HEAD 请求获取指定 Chunk 的元数据，判断其是否已经存在于 Storage Node 上
bool HeadChunk(const proto::NodeInfo& node, const proto::GetUploadReply& upload,
               const proto::PlannedChunk& chunk, long* code, uint64_t* durable,
               bool* permanentFailure, std::string* error)
{
    *permanentFailure = false;
    if (!InitCurl(error)) return false;
    Curl curl(curl_easy_init(), curl_easy_cleanup);
    if (!curl) { *error = "cannot initialize HTTP HEAD"; return false; }
    const std::string url = Url(node, chunk.chunk_id());
    Configure(curl.get(), url);
    Headers headers(nullptr, curl_slist_free_all);
    curl_slist* raw = nullptr;
    if (!AddHeader(&raw, "X-Generation: 1") ||
        !AddHeader(&raw, "X-Upload-Id: " + upload.upload_id())) {
        curl_slist_free_all(raw);
        *error = "HTTP HEAD header allocation failed";
        return false;
    }
    headers.reset(raw);
    ReplyHeaders reply;
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
    curl_easy_setopt(curl.get(), CURLOPT_NOBODY, 1L);  // 设置为 HEAD 请求，不需要响应体
    curl_easy_setopt(curl.get(), CURLOPT_HEADERFUNCTION, CaptureHeader);  // head回调
    curl_easy_setopt(curl.get(), CURLOPT_HEADERDATA, &reply);
    const CURLcode result = curl_easy_perform(curl.get());
    if (result != CURLE_OK || reply.bad) {
        *permanentFailure = reply.bad;
        *error = std::string("HTTP HEAD failed: ") + curl_easy_strerror(result);
        return false;
    }
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, code);
    if (*code != 200 && *code != 308) return true;
    if (!Text(reply, "x-chunk-id", chunk.chunk_id()) ||
        !Text(reply, "x-object-id", upload.object_id()) ||
        !Number(reply, "x-object-version", 1) ||
        !Number(reply, "x-chunk-index", chunk.index()) ||
        !Number(reply, "x-logical-offset", chunk.offset()) ||
        !Number(reply, "x-chunk-size", chunk.size()) ||
        !Text(reply, "x-checksum-sha256", chunk.sha256()) ||
        !Number(reply, "x-generation", 1)) {
        *permanentFailure = true;
        *error = "HTTP HEAD descriptor differs from upload plan";
        return false;
    }
    if (*code == 308 && (!ReadNumber(reply, "x-durable-offset", durable) ||  // 读取 durable offset
                         *durable > chunk.size())) {
        *permanentFailure = true;
        *error = "invalid durable offset in HTTP HEAD";
        return false;
    }
    return true;
}


bool GetChunk(const proto::NodeInfo& node, const proto::PlannedChunk& chunk,
              int fd, EVP_MD_CTX* fileDigest, std::string* error)
{
    if (!InitCurl(error)) return false;
    Curl curl(curl_easy_init(), curl_easy_cleanup);
    Digest chunkDigest(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!curl || !chunkDigest || EVP_DigestInit_ex(chunkDigest.get(), EVP_sha256(), nullptr) != 1) {
        *error = "cannot initialize HTTP GET or SHA-256";
        return false;
    }
    const std::string url = Url(node, chunk.chunk_id());
    Configure(curl.get(), url);
    Headers headers(curl_slist_append(nullptr, "X-Generation: 1"), curl_slist_free_all);
    if (!headers) { *error = "HTTP header allocation failed"; return false; }

    // 设置回调函数和数据结构
    ReplyHeaders reply;
    DownloadSink sink{fd, chunk.offset(), 0, chunk.size(), chunkDigest.get(), fileDigest, false};
    curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
    curl_easy_setopt(curl.get(), CURLOPT_HEADERFUNCTION, CaptureHeader);
    curl_easy_setopt(curl.get(), CURLOPT_HEADERDATA, &reply);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, WriteFile);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &sink);
    const CURLcode result = curl_easy_perform(curl.get());
    long code = 0;
    if (result == CURLE_OK) curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &code);
    if (result != CURLE_OK || reply.bad || sink.failed) {
        *error = "HTTP GET failed or exceeded expected bytes: " +
                 std::string(curl_easy_strerror(result));
        return false;
    }
    if (code != 200 || sink.received != chunk.size() ||  // 检查接收的字节数是否与chunk.size()一致
        !Number(reply, "content-length", chunk.size()) ||
        !Number(reply, "x-chunk-size", chunk.size()) ||
        !Text(reply, "x-checksum-sha256", chunk.sha256()) ||
        !Number(reply, "x-generation", chunk.generation())) {
        *error = "HTTP GET status, length or descriptor differs from committed object";
        return false;
    }

    // 检查当前chunk的SHA-256是否与计划中的SHA-256一致。
    // 参数中的fileDigest也在WriteFile中被更新了，由上层去检查整个文件的SHA-256
    std::string actual;
    if (!FinalDigest(chunkDigest.get(), &actual) || actual != chunk.sha256()) { 
        *error = "downloaded Chunk SHA-256 mismatch";
        return false;
    }
    return true;
}

}  // namespace detail
}  // namespace object_storage
