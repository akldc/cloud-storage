#ifndef OBJECT_STORAGE_NODE_STORAGENODE_H
#define OBJECT_STORAGE_NODE_STORAGENODE_H

#include "base/ThreadPool.h"
#include "net/TcpServer.h"
#include "storage/chunk_store/ChunkStore.h"

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace object_storage {

// One HTTP request per connection. Construct, start, and destroy on the
// owning EventLoop thread; stop the loop before destruction.
class StorageNode {
public:
    struct Options {
        int diskThreads{4};
        size_t diskQueueSize{64};
        size_t maxConnections{256};
        unsigned diskDelayMs{0};  // Optional fixed delay for slow-disk testing.
        unsigned idleTimeoutSeconds{60};
    };

    StorageNode(mymuduo::net::EventLoop* loop,
                const mymuduo::net::InetAddress& listenAddress,
                std::string root);
    StorageNode(mymuduo::net::EventLoop* loop,
                const mymuduo::net::InetAddress& listenAddress,
                std::string root, Options options);
    ~StorageNode();
    StorageNode(const StorageNode&) = delete;
    StorageNode& operator=(const StorageNode&) = delete;

    Status Start();
    uint16_t port() const { return server_.port(); }

private:
    struct Session;
    using Connection = mymuduo::net::TcpConnectionPtr;
    using SessionPtr = std::shared_ptr<Session>;

    void OnConnection(const Connection& connection);
    void OnMessage(const Connection& connection, mymuduo::net::Buffer* input,
                   mymuduo::Timestamp receivedAt);
    void OnWriteComplete(const Connection& connection);
    void HandleHeaders(const Connection& connection, const SessionPtr& session);
    void ConsumeUpload(const Connection& connection, const SessionPtr& session,
                       mymuduo::net::Buffer* input);
    void DiscardDuplicateBody(const Connection& connection, const SessionPtr& session,
                              mymuduo::net::Buffer* input);
    void StartUpload(const Connection& connection, const SessionPtr& session);
    void StartLookup(const Connection& connection, const SessionPtr& session);
    void StartDelete(const Connection& connection, const SessionPtr& session);
    void NextDownload(const Connection& connection, const SessionPtr& session);
    void ReleaseWriterAsync(const SessionPtr& session);
    void ReleaseReaderAsync(const SessionPtr& session);
    void SubmitUploadWindow(const Connection& connection, const SessionPtr& session,
                            std::string data, bool last);
    void Reply(const Connection& connection, const SessionPtr& session,
               int code, const std::string& reason, const std::string& body = "",
               const std::vector<std::pair<std::string, std::string>>& headers = {});

    mymuduo::net::EventLoop* loop_;
    ChunkStore store_;
    Options options_;
    std::atomic<bool> stopping_{false};
    bool started_{false};
    size_t connectionCount_{0};
    std::atomic<size_t> cleanupPending_{0};
    mymuduo::ThreadPool cleanupPool_;
    mymuduo::ThreadPool diskPool_;
    mymuduo::net::TcpServer server_;
};

}  // namespace object_storage

#endif
