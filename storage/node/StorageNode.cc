#include "storage/node/StorageNode.h"
#include "storage/node/StorageNodeHttp.h"

#include "net/HttpContext.h"
#include "net/HttpResponse.h"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <exception>
#include <limits>
#include <strings.h>
#include <thread>

namespace object_storage {
namespace {

constexpr size_t kWindow = 64 * 1024;
constexpr size_t kMaxHeaders = 16 * 1024;
using Connection = mymuduo::net::TcpConnectionPtr;
using Header = std::pair<std::string, std::string>;
using Headers = std::vector<Header>;


}  // namespace

using namespace node_detail;

struct StorageNode::Session {
    enum Phase { kHeaders, kOpening, kUpload, kDiscard, kBusy, kDownload, kDone } phase{kHeaders};
    mymuduo::net::HttpContext parser;
    std::string chunkId;
    uint64_t generation{1};
    std::string uploadId;
    BeginChunkWrite request;
    uint64_t offset{0};
    std::unique_ptr<ChunkWriter> writer; // session独占当前请求的reader/writer
    std::unique_ptr<ChunkReader> reader;
    ByteRange range;
    uint64_t sent{0};
    bool diskBusy{false};
    std::chrono::steady_clock::time_point lastActivity{std::chrono::steady_clock::now()};
    mymuduo::net::TimerId idleTimer;  // 定时器，用于检查连接是否空闲超过idleTimeoutSeconds，如果超过则关闭连接; todo
    ~Session() { assert(!writer && !reader); }
};

StorageNode::StorageNode(mymuduo::net::EventLoop* loop,
                         const mymuduo::net::InetAddress& listenAddress,
                         std::string root)
    : StorageNode(loop, listenAddress, std::move(root), Options{}) {}

StorageNode::StorageNode(mymuduo::net::EventLoop* loop,
                         const mymuduo::net::InetAddress& listenAddress,
                         std::string root, Options options)
    : loop_(loop), store_(std::move(root)), options_(options),
      cleanupPool_("storage-cleanup"), diskPool_("storage-disk"),
      server_(loop, listenAddress, "StorageNode")
{
    server_.setThreadNum(0);
    server_.setConnectionCallback([this](const Connection& c) { OnConnection(c); });  // 连接建立或断开时的回调
    server_.setMessageCallback([this](const Connection& c, mymuduo::net::Buffer* b, mymuduo::Timestamp t) { OnMessage(c, b, t); });  // 收到数据（输入buffer中有数据）
    server_.setWriteCompleteCallback([this](const Connection& c) { OnWriteComplete(c); }); // 发送数据完成时的回调
}

StorageNode::~StorageNode()
{
    loop_->assertInLoopThread();
    stopping_ = true;
    diskPool_.stop();
    loop_->drainPendingFunctors();
}

Status StorageNode::Start()
{
    loop_->assertInLoopThread();
    if (started_) return Status::Ok();
    if (options_.diskThreads <= 0 || options_.diskQueueSize == 0) {
        return {ErrorCode::kInvalidArgument, "disk pool requires workers and a bounded queue"};
    }
    // 初始化chunkStore
    auto initialized = store_.Initialize();
    if (!initialized.ok()) return initialized.status();
    if (options_.maxConnections == 0 || options_.idleTimeoutSeconds == 0) {
        return {ErrorCode::kInvalidArgument,
                "maxConnections and idleTimeoutSeconds must be positive"};
    }
    // 启动cleanupPool_和diskPool_线程池
    cleanupPool_.setMaxQueueSize(options_.maxConnections);
    cleanupPool_.start(1);
    diskPool_.setMaxQueueSize(options_.diskQueueSize);
    diskPool_.start(options_.diskThreads);
    // 启动tcpServer，监听端口，开始接收连接
    server_.start();
    started_ = true;
    return Status::Ok();
}

void StorageNode::OnConnection(const Connection& connection)
{
    if (connection->connected() && !stopping_) {
        if (connectionCount_ + cleanupPending_.load() >= options_.maxConnections) {  // 超过最大连接数，直接关闭连接
            connection->forceClose();
            return;
        }
        // 新连接建立，创建一个Session对象，并将其存储在TcpConnection的context中
        ++connectionCount_;
        auto session = std::make_shared<Session>();
        connection->setContext(session);
        std::weak_ptr<mymuduo::net::TcpConnection> weakConnection = connection;
        std::weak_ptr<Session> weakSession = session;
        session->idleTimer = loop_->runEvery( // 每隔一段时间 检查最后活跃时间是否超过 idleTimeoutSeconds，如果超过则关闭连接
            static_cast<double>(std::min(5u, options_.idleTimeoutSeconds)),
            [this, weakConnection, weakSession] {
                auto c = weakConnection.lock();
                auto s = weakSession.lock();
                if (c && s && c->connected() &&
                    std::chrono::steady_clock::now() - s->lastActivity >=
                        std::chrono::seconds(options_.idleTimeoutSeconds)) {
                    c->forceClose();
                }
            });
    } else { // 连接断开，清理Session对象，释放资源
        auto session = std::static_pointer_cast<Session>(connection->getContext());
        if (session) {
            loop_->cancel(session->idleTimer);
            --connectionCount_;
            if (session->phase != Session::kBusy && session->phase != Session::kOpening) {
                ReleaseWriterAsync(session);
                if (!session->diskBusy) ReleaseReaderAsync(session);
            }
        }
        connection->setContext({});
    }
}

// 当收到数据时的回调函数，根据session的phase来处理不同的请求阶段
void StorageNode::OnMessage(const Connection& connection, mymuduo::net::Buffer* input,
                            mymuduo::Timestamp receivedAt)
{
    if (stopping_) return;
    // 取出session
    auto session = std::static_pointer_cast<Session>(connection->getContext());
    if (!session) return;
    session->lastActivity = std::chrono::steady_clock::now();
    if (session->phase == Session::kHeaders) {
        auto parsed = session->parser.parseRequest(input, receivedAt, true);
        size_t headerBytes = parsed == mymuduo::net::HttpContext::kNeedMore
                                 ? input->readableBytes() : 0;
        headerBytes += session->parser.request().path().size();
        for (const auto& field : session->parser.request().headers()) {
            headerBytes += field.first.size() + field.second.size() + 4;
        }
        if (parsed == mymuduo::net::HttpContext::kError || headerBytes > kMaxHeaders) {
            Reply(connection, session, 400, Reason(400));
        } else if (parsed == mymuduo::net::HttpContext::kGotRequest ||
                   parsed == mymuduo::net::HttpContext::kHeadersComplete) {
            connection->stopRead(); // todo
            HandleHeaders(connection, session); // 解析完成，处理Head, 根据method分发处理函数
        }
    } else if (session->phase == Session::kUpload) {
        ConsumeUpload(connection, session, input);
    } else if (session->phase == Session::kDiscard) {
        DiscardDuplicateBody(connection, session, input);
    } else if (session->phase == Session::kDone) {
        input->retrieveAll();
    }
}

// 根据http method和请求头，分发请求到不同的处理函数（上传、下载、删除等）
void StorageNode::HandleHeaders(const Connection& connection, const SessionPtr& session)
{
    const auto& request = session->parser.request();
    const std::string& path = request.path();
    constexpr const char* prefix = "/v1/chunks/";
    if (path.compare(0, 11, prefix) != 0 || !IsCanonicalId(path.substr(11))) {
        Reply(connection, session, 404, Reason(404));
        return;
    }
    session->chunkId = path.substr(11);
    if (!request.getHeader("Transfer-Encoding").empty()) {
        Reply(connection, session, 400, Reason(400));
        return;
    }
    if (request.method() == mymuduo::net::HttpRequest::kPut) {  // PUT /v1/chunks/<chunkId>  上传
        if (!ParseUploadRequest(request, session->chunkId, &session->request, &session->offset) ||   // session->request保存了上传请求的参数，session->offset是本请求在chunk的起始偏移
            session->parser.contentLength() > session->request.descriptor.size - session->offset) {
            Reply(connection, session, 400, Reason(400));
            return;
        }
        StartUpload(connection, session);  // 开始上传
        return;
    }
    if (session->parser.contentLength() != 0 ||
        !ParseUint(request.getHeader("X-Generation"), &session->generation) ||
        session->generation == 0) {
        Reply(connection, session, 400, Reason(400));
        return;
    }
    if (request.method() == mymuduo::net::HttpRequest::kHead ||
        request.method() == mymuduo::net::HttpRequest::kGet) { // GET/HEAD /v1/chunks/<chunkId>
        session->uploadId = request.getHeader("X-Upload-Id");
        StartLookup(connection, session);
    } else if (request.method() == mymuduo::net::HttpRequest::kDelete) {  // DELETE /v1/chunks/<chunkId>
        StartDelete(connection, session);
    } else {
        Reply(connection, session, 400, Reason(400));
    }
}

void StorageNode::Reply(const Connection& connection, const SessionPtr& session,
                        int code, const std::string& reason, const std::string& body,
                        const Headers& headers)
{
    session->phase = Session::kDone;
    ReleaseWriterAsync(session);
    ReleaseReaderAsync(session);
    connection->stopRead();
    mymuduo::net::HttpResponse response(true);
    response.setStatusCode(static_cast<mymuduo::net::HttpResponse::HttpStatusCode>(code));
    response.setStatusMessage(reason);
    response.setBody(body);
    for (const auto& header : headers) response.addHeader(header.first, header.second);
    mymuduo::net::Buffer output;
    response.appendToBuffer(&output);
    connection->send(&output);
}

void StorageNode::ReleaseWriterAsync(const SessionPtr& session)
{
    if (!session->writer) return;
    auto writer = std::make_shared<std::unique_ptr<ChunkWriter>>(std::move(session->writer));
    ++cleanupPending_;
    if (!cleanupPool_.tryRun([this, writer] {
            writer->reset();
            --cleanupPending_;
        })) {
        // Admission caps concurrent connections and pending releases, so this
        // bounded queue cannot fill while the cleanup worker is running.
        std::terminate();
    }
}

void StorageNode::ReleaseReaderAsync(const SessionPtr& session)
{
    if (!session->reader) return;
    auto reader = std::make_shared<std::unique_ptr<ChunkReader>>(std::move(session->reader));
    ++cleanupPending_;
    if (!cleanupPool_.tryRun([this, reader] {
            reader->reset();
            --cleanupPending_;
        })) {
        std::terminate();
    }
}

void StorageNode::StartUpload(const Connection& connection, const SessionPtr& session)
{
    // session状态切换为kOpening，表示正在尝试打开chunk进行写入
    session->phase = Session::kOpening;
    std::weak_ptr<mymuduo::net::TcpConnection> weak = connection;
    auto outcome = std::make_shared<Status>();
    auto task = [this, session, weak, outcome] {
        auto opened = store_.BeginWrite(session->request);  // opened 是一个 Result<std::unique_ptr<ChunkWriter>> 对象，表示尝试开始写入chunk的结果
        if (!opened.ok() && opened.status().code() == ErrorCode::kWriteAttemptExists) {  // 尝试resume写入
            opened = store_.ResumeWrite({session->request.uploadId,
                                         {session->chunkId, session->request.descriptor.generation}});
        }
        *outcome = opened.status();
        if (opened.ok()) session->writer = std::move(opened).value();  // session正式拥有writer

        // 通知eventloop线程，writer准备好，可以开始上传数据了
        loop_->queueInLoop([this, session, weak, outcome] {
            auto c = weak.lock();
            if (!c || !c->connected() || stopping_) {
                ReleaseWriterAsync(session);
                return;
            }
            if (outcome->code() == ErrorCode::kAlreadyCommitted) {
                session->phase = Session::kDiscard;
                DiscardDuplicateBody(c, session, c->inputBuffer());
                return;
            }
            if (!outcome->ok()) {
                Reply(c, session, HttpCode(*outcome), Reason(HttpCode(*outcome)));
                return;
            }
            // 检查writer的descriptor和请求的descriptor是否一致
            // 检查请求的offset是否和writer已经接收的字节数一致，确保上传数据是连续的
            if (!(session->writer->descriptor() == session->request.descriptor) ||
                session->writer->receivedBytes() != session->offset) {
                Reply(c, session, 409, Reason(409), "",
                      {{"X-Durable-Offset", std::to_string(session->writer->durableBytes())}});
                return;
            }
            // 校验通过，修改状态，开始上传数据
            session->phase = Session::kUpload;
            ConsumeUpload(c, session, c->inputBuffer()); 
        });
    };
    if (!diskPool_.tryRun(std::move(task))) Reply(connection, session, 503, Reason(503));
}

void StorageNode::DiscardDuplicateBody(const Connection& connection,
                                       const SessionPtr& session,
                                       mymuduo::net::Buffer* input)
{
    const size_t take = std::min(input->readableBytes(), session->parser.remainingLength());
    if (take != 0) {
        input->retrieve(take);
        if (!session->parser.consumeBody(take)) {
            Reply(connection, session, 400, Reason(400));
            return;
        }
    }
    if (session->parser.gotAll()) {
        Reply(connection, session, 200, Reason(200), "",
              DescriptorHeaders(session->request.descriptor));
    } else {
        connection->startRead();
    }
}

void StorageNode::ConsumeUpload(const Connection& connection, const SessionPtr& session,
                                mymuduo::net::Buffer* input)
{
    if (session->parser.gotAll()) {
        SubmitUploadWindow(connection, session, "", true);
        return;
    }
    // 计算本次可以消费的字节数，取input中可读字节数、parser剩余长度（HTTP请求体尚未消费的长度）和kWindow的最小值
    const size_t take = std::min({input->readableBytes(),
                                  session->parser.remainingLength(), kWindow});
    if (take == 0) {
        connection->startRead(); // 当前 TcpConnection 的输入 Buffer 中已经没有可消费的数据，但 HTTP Body 还没有全部接收完成
        return;
    }
    // 从网络buffer中拷贝数据到data
    // 输入 Buffer 属于 TcpConnection，由 EventLoop 管理。磁盘任务会在另一个线程异步执行，不能直接长期引用可能发生变化的网络 Buffer 内存
    std::string data(input->peek(), take);
    input->retrieve(take);
    const bool consumed = session->parser.consumeBody(take);
    if (!consumed) {
        Reply(connection, session, 400, Reason(400));
        return;
    }
    SubmitUploadWindow(connection, session, std::move(data), session->parser.gotAll());
}

// 上传链路的核心函数
void StorageNode::SubmitUploadWindow(const Connection& connection, const SessionPtr& session,
                                     std::string data, bool last)
{
    session->phase = Session::kBusy;
    connection->stopRead();  // 当前数据正在等待磁盘处理，暂时不要继续从socket读取数据，避免内存占用过大
    struct ResultData {
        Status status;
        uint64_t expected{0};
        uint64_t durable{0};
        bool committed{false};
        bool uncertain{false};
    };
    auto result = std::make_shared<ResultData>();
    std::weak_ptr<mymuduo::net::TcpConnection> weak = connection;
    auto task = [this, session, weak, result, data = std::move(data), last] {
        if (options_.diskDelayMs != 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(options_.diskDelayMs));
        }
        ChunkWriter& writer = *session->writer;
        if (!data.empty()) {
            auto appended = writer.Append(session->offset, data.data(), data.size());  // Append
            result->status = appended.status;
            result->expected = appended.expectedOffset;
            result->durable = appended.durableOffset;
        } else {
            result->expected = writer.receivedBytes();
            result->durable = writer.durableBytes();
        }
        if (result->status.ok()) {  // 写入成功
            result->status = writer.Checkpoint();  // 每次写入后都做一次checkpoint，确保数据持久化（写入到磁盘并刷新）代价很大
            result->expected = writer.receivedBytes();
            result->durable = writer.durableBytes();
        }
        if (result->status.ok() && last &&
            writer.receivedBytes() == session->request.descriptor.size) { // 最后一块数据 && 已经写入的字节数等于chunk的总大小，则提交chunk
            auto committed = writer.Commit();  // 提交
            result->status = committed.status();
            result->committed = committed.ok();
            if (!committed.ok() && committed.status().code() == ErrorCode::kChecksumMismatch) {
                Status aborted = writer.Abort();
                if (!aborted.ok()) result->uncertain = true;
            } else if (!committed.ok()) {
                result->uncertain = true;   // commit阶段发生错误，无法确定最终 Chunk 是否已经完成发布。选择让client端后续通过head查询实际状态
            }
        }
        loop_->queueInLoop([this, session, weak, result, last] {  // 通过queueInLoop将结果回调到主线程
            auto c = weak.lock();
            if (!c || !c->connected() || stopping_) {
                ReleaseWriterAsync(session);
                return;
            }
            if (!result->status.ok()) {
                const int code = result->uncertain ? 503 : HttpCode(result->status);
                Headers headers{{"X-Durable-Offset", std::to_string(result->durable)}};  // 磁盘写入失败，返回503或其他错误码，并告知客户端当前chunk的持久化偏移量
                if (result->uncertain) headers.emplace_back("X-Commit-State", "unknown");
                Reply(c, session, code, Reason(code), "", headers);
                return;
            }
            // 成功append数据，更新session的offset为writer的receivedBytes()
            // 下一次提交从session->offset开始，确保数据连续写入
            session->offset = result->expected;
            if (last) {
                const int code = result->committed ? 201 : 202;
                Reply(c, session, code, Reason(code), "",
                      {{"X-Durable-Offset", std::to_string(result->durable)}});
            } else {
                // 继续消费上传数据
                session->phase = Session::kUpload;
                ConsumeUpload(c, session, c->inputBuffer());
            }
        });
    };
    if (!diskPool_.tryRun(std::move(task))) Reply(connection, session, 503, Reason(503));
}



void StorageNode::StartLookup(const Connection& connection, const SessionPtr& session)
{
    session->phase = Session::kOpening;
    struct ResultData {
        Status status;
        ChunkDescriptor descriptor;
        std::unique_ptr<ChunkReader> reader;
        ByteRange range;
        uint64_t durable{0};
        bool partial{false};
        bool staging{false};
        bool invalidRange{false};
    };
    auto result = std::make_shared<ResultData>();
    std::weak_ptr<mymuduo::net::TcpConnection> weak = connection;
    const bool head = session->parser.request().method() == mymuduo::net::HttpRequest::kHead;
    const std::string rangeHeader = session->parser.request().getHeader("Range");
    auto task = [this, session, weak, result, head, rangeHeader] {
        auto stat = store_.Stat({session->chunkId, session->generation});
        result->status = stat.status();
        if (stat.ok()) {
            result->descriptor = stat.value().descriptor;
            if (!head) {
                if (!ParseRange(rangeHeader, result->descriptor.size, &result->range)) {  // 支持Range请求，解析Range头部，获取请求的字节范围
                    result->invalidRange = true;
                } else {  // GET请求，且chunk存在，尝试打开chunk的reader
                    auto opened = store_.OpenRead({session->chunkId, session->generation},
                                                  result->range);
                    result->status = opened.status();
                    if (opened.ok()) result->reader = std::move(opened).value();
                    result->partial = !rangeHeader.empty();
                }
            }
        } else if (head && stat.status().code() == ErrorCode::kNotFound &&  // todo
                   IsCanonicalId(session->uploadId)) {  // 如果是HEAD请求，并且chunk不存在，且提供了合法的uploadId，则尝试resume写入，返回staging状态
            auto resumed = store_.ResumeWrite({session->uploadId,
                                                {session->chunkId, session->generation}});
            result->status = resumed.status();
            if (resumed.ok()) {
                result->staging = true;
                result->durable = resumed.value()->durableBytes();
                result->descriptor = resumed.value()->descriptor();
            }
        }
        loop_->queueInLoop([this, session, weak, result, head] {
            session->reader = std::move(result->reader);
            auto c = weak.lock();
            if (!c || !c->connected() || stopping_) {
                ReleaseReaderAsync(session);
                return;
            }
            if (result->invalidRange) {
                Reply(c, session, 416, Reason(416), "",
                      {{"Content-Range", "bytes */" + std::to_string(result->descriptor.size)}});
                return;
            }
            if (!result->status.ok()) {
                Reply(c, session, HttpCode(result->status), Reason(HttpCode(result->status)));
                return;
            }
            Headers headers = DescriptorHeaders(result->descriptor);
            if (result->staging) {
                headers.emplace_back("X-Durable-Offset", std::to_string(result->durable));
                Reply(c, session, 308, Reason(308), "", headers);
                return;
            }
            if (head) { // 对于HEAD,直接构造相应
                headers.emplace_back("Content-Length", std::to_string(result->descriptor.size));
                Reply(c, session, 200, Reason(200), "", headers);
                return;
            }
            session->range = result->range;
            session->phase = Session::kDownload;
            const int code = result->partial ? 206 : 200;
            headers.emplace_back("Content-Length", std::to_string(result->range.length));
            headers.emplace_back("Content-Type", "application/octet-stream");
            if (result->partial) {
                headers.emplace_back("Content-Range",
                    "bytes " + std::to_string(result->range.offset) + "-" +
                    std::to_string(result->range.offset + result->range.length - 1) + "/" +
                    std::to_string(result->descriptor.size));
            }
            mymuduo::net::HttpResponse response(true);
            response.setStatusCode(static_cast<mymuduo::net::HttpResponse::HttpStatusCode>(code));
            response.setStatusMessage(Reason(code));
            for (const auto& h : headers) response.addHeader(h.first, h.second);
            mymuduo::net::Buffer output;
            response.appendToBuffer(&output);
            c->send(&output); // 发送响应头？？
            if (session->range.length == 0) session->phase = Session::kDone;
            else NextDownload(c, session);
        });
    };
    if (!diskPool_.tryRun(std::move(task))) Reply(connection, session, 503, Reason(503));
}

// 下载的核心函数，负责从chunk中读取数据，并发送给客户端
void StorageNode::NextDownload(const Connection& connection, const SessionPtr& session)
{
    if (session->diskBusy || session->sent >= session->range.length) return;
    session->diskBusy = true;
    const uint64_t offset = session->sent; // ??
    auto data = std::make_shared<std::string>(static_cast<size_t>(  // 数据缓冲区
        std::min<uint64_t>(kWindow, session->range.length - offset)), '\0');
    auto status = std::make_shared<Status>();
    std::weak_ptr<mymuduo::net::TcpConnection> weak = connection;
    auto task = [this, session, weak, data, status, offset] {
        if (options_.diskDelayMs != 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(options_.diskDelayMs));
        }
        auto read = session->reader->ReadAt(offset, data->data(), data->size()); // 从chunk中读取数据到data缓冲区
        *status = read.status();
        if (read.ok()) data->resize(read.value());
        loop_->queueInLoop([this, session, weak, data, status] {
            auto c = weak.lock();
            if (!c || !c->connected() || stopping_) {
                ReleaseReaderAsync(session);
                return;
            }
            session->diskBusy = false;
            if (!status->ok() || data->empty()) {
                c->forceClose();
                return;
            }
            session->sent += data->size();
            c->send(data->data(), static_cast<int>(data->size()));
        });
    };
    if (!diskPool_.tryRun(std::move(task))) {
        session->diskBusy = false;
        ReleaseReaderAsync(session);
        connection->forceClose();
    }
}

void StorageNode::OnWriteComplete(const Connection& connection)
{
    if (stopping_) return;
    auto session = std::static_pointer_cast<Session>(connection->getContext());
    if (!session) return;
    session->lastActivity = std::chrono::steady_clock::now();
    if (session->phase == Session::kDone) {
        ReleaseReaderAsync(session);
        connection->startRead();
        connection->shutdown();
        std::weak_ptr<mymuduo::net::TcpConnection> weak = connection;
        loop_->runAfter(5.0, [weak] {
            if (auto c = weak.lock()) c->forceClose();
        });
        return;
    }
    if (session->phase != Session::kDownload || session->diskBusy) return;
    // 正在下载，且没有等待磁盘操作的session, 继续下载下一块数据
    // tcpconnection的应用层发送缓冲区已经排空
    if (session->sent == session->range.length) {
        session->phase = Session::kDone;
        OnWriteComplete(connection);
    }
    else NextDownload(connection, session);
}

void StorageNode::StartDelete(const Connection& connection, const SessionPtr& session)
{
    session->phase = Session::kOpening;
    auto status = std::make_shared<Status>();
    std::weak_ptr<mymuduo::net::TcpConnection> weak = connection;
    auto task = [this, session, weak, status] {
        *status = store_.Delete({session->chunkId, session->generation});
        loop_->queueInLoop([this, session, weak, status] {
            auto c = weak.lock();
            if (!c || !c->connected() || stopping_) return;
            const int code = status->ok() ? 204 : HttpCode(*status);
            Reply(c, session, code, Reason(code));
        });
    };
    if (!diskPool_.tryRun(std::move(task))) Reply(connection, session, 503, Reason(503));
}

}  // namespace object_storage
