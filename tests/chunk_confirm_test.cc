#include "coordinator/CoordinatorService.h"
#include "coordinator/ObjectCatalog.h"

#include <grpcpp/grpcpp.h>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace object_storage::coordinator;
namespace proto = object_storage::control::v1;

namespace {
const std::string requestId = "11111111-1111-4111-8111-111111111111";
const std::string nodeId = "22222222-2222-4222-8222-222222222222";
const std::string instanceId = "33333333-3333-4333-8333-333333333333";
const std::string sha(64, 'a');

using Fields = std::vector<std::pair<std::string, std::string>>;

class FakeHead {
public:
    FakeHead()
    {
        fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        assert(fd_ >= 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        assert(::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof address) == 0);
        assert(::listen(fd_, 16) == 0);
        socklen_t length = sizeof address;
        assert(::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) == 0);
        port_ = ntohs(address.sin_port);
        worker_ = std::thread([this] { Serve(); });
    }
    ~FakeHead()
    {
        stopping_ = true;
        worker_.join();
        ::close(fd_);
    }
    uint16_t port() const { return port_; }
    int calls() const { return calls_; }
    bool validRequests() const { return validRequests_; }
    void ExpectChunk(std::string chunk)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        chunk_ = std::move(chunk);
    }
    void Reply(int status, Fields fields, int delayMs = 0)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        status_ = status;
        fields_ = std::move(fields);
        delayMs_ = delayMs;
    }

private:
    void Serve()
    {
        while (!stopping_) {
            pollfd ready{fd_, POLLIN, 0};
            if (::poll(&ready, 1, 20) <= 0) continue;
            const int client = ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC);
            if (client < 0) continue;
            std::string request;
            char data[1024];
            while (request.find("\r\n\r\n") == std::string::npos && request.size() < 8192) {
                const ssize_t count = ::recv(client, data, sizeof data, 0);
                if (count <= 0) break;
                request.append(data, static_cast<size_t>(count));
            }
            int status, delay;
            Fields fields;
            std::string chunk;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                status = status_;
                delay = delayMs_;
                fields = fields_;
                chunk = chunk_;
            }
            if (request.find("HEAD /v1/chunks/" + chunk + " HTTP/") != 0 ||
                request.find("X-Generation: 1\r\n") == std::string::npos) {
                validRequests_ = false;
            }
            ++calls_;
            if (delay) std::this_thread::sleep_for(std::chrono::milliseconds(delay));
            std::string response = "HTTP/1.1 " + std::to_string(status) + " Test\r\n";
            for (const auto& field : fields) response += field.first + ": " + field.second + "\r\n";
            response += "Content-Length: 0\r\nConnection: close\r\n\r\n";
            ::send(client, response.data(), response.size(), MSG_NOSIGNAL);
            ::close(client);
        }
    }
    int fd_{-1};
    uint16_t port_{0};
    std::thread worker_;
    std::atomic<bool> stopping_{false};
    std::atomic<int> calls_{0};
    std::atomic<bool> validRequests_{true};
    std::mutex mutex_;
    std::string chunk_;
    int status_{404};
    int delayMs_{0};
    Fields fields_;
};

grpc::Status Confirm(proto::Coordinator::Stub* stub, const std::string& upload)
{
    proto::ConfirmChunkRequest request;
    request.set_upload_id(upload);
    request.set_chunk_index(0);
    proto::ConfirmChunkReply reply;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    return stub->ConfirmChunk(&context, request, &reply);
}

void Run(const std::string& path)
{
    FakeHead head;
    NodeRegistry registry;
    assert(registry.Register(nodeId, instanceId, "127.0.0.1", head.port(), Clock::now()).ok());
    ObjectCatalog catalog(path);
    BeginUploadInput input{requestId, 1, sha, {{0, 0, 1, sha}}};
    auto upload = catalog.BeginUpload(input, registry.OnlineNodes(Clock::now()));
    assert(upload.ok());
    const auto& plan = upload.value->chunks[0];
    head.ExpectChunk(plan.chunkId);
    Fields correct = {{"X-Chunk-Id", plan.chunkId}, {"X-Object-Id", upload.value->objectId},
                      {"X-Object-Version", "1"}, {"X-Chunk-Index", "0"},
                      {"X-Logical-Offset", "0"}, {"X-Chunk-Size", "1"},
                      {"X-Checksum-Sha256", sha}, {"X-Generation", "1"}};

    CoordinatorService service(&registry, 2000, 10000, &catalog, 100);
    grpc::ServerBuilder builder;
    builder.RegisterService(&service);
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    auto server = builder.BuildAndStart();
    assert(server && port > 0);
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    auto stub = proto::Coordinator::NewStub(grpc::CreateCustomChannel(
        "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials(), args));

    for (size_t i = 0; i < correct.size(); ++i) {
        Fields wrong = correct;
        wrong[i].second = i == 0 || i == 1 ? "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" :
                          i == 6 ? std::string(64, 'b') : "2";
        head.Reply(200, wrong);
        assert(Confirm(stub.get(), upload.value->uploadId).error_code() == grpc::FAILED_PRECONDITION);
        assert(catalog.GetUpload(requestId).value->chunks[0].confirmedNodeId.empty());
    }
    Fields missing = correct;
    missing.pop_back();
    head.Reply(200, missing);
    assert(Confirm(stub.get(), upload.value->uploadId).error_code() == grpc::FAILED_PRECONDITION);
    Fields malformed = correct;
    malformed[3].second = "not-a-number";
    head.Reply(200, malformed);
    assert(Confirm(stub.get(), upload.value->uploadId).error_code() == grpc::FAILED_PRECONDITION);
    for (int code : {308, 404, 409, 503}) {
        head.Reply(code, correct);
        auto status = Confirm(stub.get(), upload.value->uploadId);
        assert(status.error_code() == (code == 503 ? grpc::UNAVAILABLE : grpc::FAILED_PRECONDITION));
    }
    head.Reply(200, correct, 250);
    assert(Confirm(stub.get(), upload.value->uploadId).error_code() == grpc::UNAVAILABLE);
    assert(catalog.GetUpload(requestId).value->chunks[0].confirmedNodeId.empty());
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    assert(registry.Unregister(nodeId, instanceId).ok());
    head.Reply(200, correct);
    assert(Confirm(stub.get(), upload.value->uploadId).error_code() == grpc::UNAVAILABLE);
    const std::string replacementInstance = "44444444-4444-4444-8444-444444444444";
    assert(registry.Register(nodeId, replacementInstance, "127.0.0.1", head.port(),
                             Clock::now()).ok());
    head.Reply(200, correct);
    assert(Confirm(stub.get(), upload.value->uploadId).ok());
    assert(catalog.GetUpload(requestId).value->chunks[0].confirmedNodeId == nodeId);
    const int before = head.calls();
    head.Reply(404, {});
    assert(Confirm(stub.get(), upload.value->uploadId).ok());
    assert(head.calls() == before);
    assert(head.validRequests());

    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
    server->Wait();
}
}  // namespace

int main()
{
    char path[] = "/tmp/chunk-confirm-test-XXXXXX";
    char* dir = ::mkdtemp(path);
    assert(dir);
    const std::string directory(dir);
    Run(directory + "/objects.sqlite");
    std::filesystem::remove_all(directory);
}
