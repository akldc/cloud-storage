#include "coordinator/CoordinatorService.h"

#include <grpcpp/grpcpp.h>

#include <cassert>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace object_storage::coordinator;
namespace proto = object_storage::control::v1;
using namespace std::chrono_literals;

namespace {

const std::string instanceA = "aaaaaaaa-0000-0000-0000-000000000000";
const std::string instanceB = "bbbbbbbb-0000-0000-0000-000000000000";
const std::string chunk = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

std::string Id(unsigned n)
{
    char buffer[37];
    std::snprintf(buffer, sizeof buffer, "%08x-0000-0000-0000-000000000000", n);
    return buffer;
}

void SetDeadline(grpc::ClientContext* context)
{
    context->set_deadline(std::chrono::system_clock::now() + 2s);
}

class ServerFixture {
public:
    ServerFixture() : service_(&registry_, 2000, 10000)
    {
        grpc::ServerBuilder builder;
        builder.RegisterService(&service_);
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        server_ = builder.BuildAndStart();
        assert(server_ && port != 0);
        grpc::ChannelArguments args;
        args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
        channel_ = grpc::CreateCustomChannel("127.0.0.1:" + std::to_string(port),
                                             grpc::InsecureChannelCredentials(), args);
        stub_ = proto::Coordinator::NewStub(channel_);
    }
    ~ServerFixture()
    {
        server_->Shutdown(std::chrono::system_clock::now() + 3s);
        server_->Wait();
    }

    NodeRegistry& registry() { return registry_; }
    proto::Coordinator::Stub& stub() { return *stub_; }
    std::shared_ptr<grpc::Channel> channel() { return channel_; }

private:
    NodeRegistry registry_;
    CoordinatorService service_;
    std::unique_ptr<grpc::Server> server_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<proto::Coordinator::Stub> stub_;
};

grpc::Status Register(proto::Coordinator::Stub& stub, const std::string& node,
                      const std::string& instance, const std::string& host, uint32_t port,
                      proto::RegisterNodeReply* reply = nullptr)
{
    proto::RegisterNodeRequest request;
    request.set_node_id(node);
    request.set_instance_id(instance);
    request.set_host(host);
    request.set_port(port);
    proto::RegisterNodeReply local;
    grpc::ClientContext context;
    SetDeadline(&context);
    return stub.RegisterNode(&context, request, reply ? reply : &local);
}

grpc::Status Heartbeat(proto::Coordinator::Stub& stub, const std::string& node,
                       const std::string& instance)
{
    proto::HeartbeatRequest request;
    request.set_node_id(node);
    request.set_instance_id(instance);
    proto::HeartbeatReply reply;
    grpc::ClientContext context;
    SetDeadline(&context);
    return stub.Heartbeat(&context, request, &reply);
}

grpc::Status Unregister(proto::Coordinator::Stub& stub, const std::string& node,
                        const std::string& instance)
{
    proto::UnregisterNodeRequest request;
    request.set_node_id(node);
    request.set_instance_id(instance);
    proto::UnregisterNodeReply reply;
    grpc::ClientContext context;
    SetDeadline(&context);
    return stub.UnregisterNode(&context, request, &reply);
}

grpc::Status GetNode(proto::Coordinator::Stub& stub, const std::string& node,
                     proto::NodeInfo* reply)
{
    proto::GetNodeRequest request;
    request.set_node_id(node);
    grpc::ClientContext context;
    SetDeadline(&context);
    return stub.GetNode(&context, request, reply);
}

grpc::Status ListNodes(proto::Coordinator::Stub& stub, proto::ListNodesReply* reply)
{
    proto::ListNodesRequest request;
    grpc::ClientContext context;
    SetDeadline(&context);
    return stub.ListNodes(&context, request, reply);
}

grpc::Status Placement(proto::Coordinator::Stub& stub, const std::string& id,
                       uint32_t count, proto::GetPlacementReply* reply)
{
    proto::GetPlacementRequest request;
    request.set_chunk_id(id);
    request.set_replica_count(count);
    grpc::ClientContext context;
    SetDeadline(&context);
    return stub.GetPlacement(&context, request, reply);
}

void Expect(const grpc::Status& status, grpc::StatusCode code)
{
    if (status.error_code() != code) {
        std::fprintf(stderr, "expected gRPC code %d, got %d: %s\n",
                     static_cast<int>(code), static_cast<int>(status.error_code()),
                     status.error_message().c_str());
    }
    assert(status.error_code() == code);
}

void TestRpcContract()
{
    ServerFixture server;
    auto& stub = server.stub();
    proto::NodeInfo node;
    proto::GetPlacementReply candidates;
    proto::ListNodesReply list;
    proto::RegisterNodeReply registered;

    Expect(Register(stub, "", instanceA, "host", 8000), grpc::INVALID_ARGUMENT);
    Expect(Register(stub, Id(1), "", "host", 8000), grpc::INVALID_ARGUMENT);
    Expect(Register(stub, Id(1), instanceA, "", 8000), grpc::INVALID_ARGUMENT);
    Expect(Register(stub, Id(1), instanceA, "bad_host", 8000), grpc::INVALID_ARGUMENT);
    Expect(Register(stub, Id(1), instanceA, "0.0.0.0", 8000), grpc::INVALID_ARGUMENT);
    Expect(Register(stub, Id(1), instanceA, "host", 0), grpc::INVALID_ARGUMENT);
    Expect(Register(stub, Id(1), instanceA, "host", 65536), grpc::INVALID_ARGUMENT);
    Expect(Heartbeat(stub, "", instanceA), grpc::INVALID_ARGUMENT);
    Expect(Unregister(stub, Id(1), ""), grpc::INVALID_ARGUMENT);
    Expect(GetNode(stub, "", &node), grpc::INVALID_ARGUMENT);
    Expect(Placement(stub, "", 1, &candidates), grpc::INVALID_ARGUMENT);
    Expect(Placement(stub, chunk, 0, &candidates), grpc::INVALID_ARGUMENT);
    Expect(Placement(stub, chunk, 17, &candidates), grpc::INVALID_ARGUMENT);
    Expect(GetNode(stub, Id(1), &node), grpc::NOT_FOUND);
    Expect(Heartbeat(stub, Id(1), instanceA), grpc::NOT_FOUND);
    Expect(Unregister(stub, Id(1), instanceA), grpc::NOT_FOUND);

    Expect(Register(stub, Id(1), instanceA, "NODE.Example", 8000, &registered), grpc::OK);
    assert(registered.heartbeat_interval_ms() == 2000 && registered.lease_timeout_ms() == 10000);
    Expect(Register(stub, Id(1), instanceA, "node.example", 8000), grpc::OK);
    Expect(Register(stub, Id(1), instanceA, "other.example", 8000),
           grpc::FAILED_PRECONDITION);
    Expect(Register(stub, Id(1), instanceB, "node.example", 8000), grpc::ALREADY_EXISTS);
    Expect(Register(stub, Id(2), instanceA, "NODE.EXAMPLE", 8000), grpc::ALREADY_EXISTS);
    Expect(Heartbeat(stub, Id(1), instanceB), grpc::FAILED_PRECONDITION);
    Expect(Unregister(stub, Id(1), instanceB), grpc::FAILED_PRECONDITION);
    Expect(Heartbeat(stub, Id(1), instanceA), grpc::OK);

    Expect(GetNode(stub, Id(1), &node), grpc::OK);
    assert(node.node_id() == Id(1) && node.host() == "node.example" &&
           node.state() == proto::NODE_ONLINE && node.last_seen_unix_ms() > 0);
    Expect(ListNodes(stub, &list), grpc::OK);
    assert(list.nodes_size() == 1 && list.nodes(0).node_id() == Id(1));
    Expect(Placement(stub, chunk, 2, &candidates), grpc::FAILED_PRECONDITION);
    assert(candidates.candidates_size() == 0);
    Expect(Register(stub, Id(2), instanceA, "second.example", 8001), grpc::OK);
    Expect(Placement(stub, chunk, 2, &candidates), grpc::OK);
    assert(candidates.candidates_size() == 2 &&
           candidates.candidates(0).node_id() != candidates.candidates(1).node_id());
    assert(candidates.candidates(0).state() == proto::NODE_ONLINE);

    Expect(Unregister(stub, Id(1), instanceA), grpc::OK);
    Expect(Unregister(stub, Id(1), instanceA), grpc::OK);
    Expect(Heartbeat(stub, Id(1), instanceA), grpc::NOT_FOUND);
    Expect(GetNode(stub, Id(1), &node), grpc::OK);
    assert(node.state() == proto::NODE_OFFLINE);
    Expect(Register(stub, Id(3), instanceA, "node.example", 8000), grpc::OK);
}

void TestEvictionOverRpc()
{
    ServerFixture server;
    const auto now = Clock::now();
    // One historical record is expired; the other 1023 remain online.
    assert(server.registry().Register(Id(5000), instanceA, "old", 8000, now - 20s).ok());
    for (unsigned i = 1; i <= 1023; ++i) {
        assert(server.registry().Register(Id(i), instanceA,
                                          "host" + std::to_string(i), 8000, now).ok());
    }
    auto& stub = server.stub();
    proto::NodeInfo node;
    Expect(Register(stub, Id(6000), instanceA, "host1", 8000), grpc::ALREADY_EXISTS);
    Expect(GetNode(stub, Id(5000), &node), grpc::OK);
    assert(node.state() == proto::NODE_OFFLINE);
    Expect(Register(stub, Id(6000), instanceA, "fresh", 8000), grpc::OK);
    Expect(GetNode(stub, Id(5000), &node), grpc::NOT_FOUND);
    proto::ListNodesReply list;
    Expect(ListNodes(stub, &list), grpc::OK);
    assert(list.nodes_size() == 1024);
    Expect(Register(stub, Id(6001), instanceA, "another", 8000), grpc::RESOURCE_EXHAUSTED);
}

void TestConcurrentRpc()
{
    ServerFixture server;
    std::vector<std::thread> workers;
    for (unsigned worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker] {
            auto stub = proto::Coordinator::NewStub(server.channel());
            for (unsigned i = 1; i <= 30; ++i) {
                const unsigned n = 100 + worker * 30 + i;
                Expect(Register(*stub, Id(n), instanceA, "node" + std::to_string(n), 9000),
                       grpc::OK);
                Expect(Heartbeat(*stub, Id(n), instanceA), grpc::OK);
                proto::NodeInfo node;
                Expect(GetNode(*stub, Id(n), &node), grpc::OK);
                assert(node.state() == proto::NODE_ONLINE);
                proto::ListNodesReply list;
                Expect(ListNodes(*stub, &list), grpc::OK);
                proto::GetPlacementReply candidates;
                Expect(Placement(*stub, chunk, 1, &candidates), grpc::OK);
                assert(candidates.candidates_size() == 1);
            }
        });
    }
    for (auto& worker : workers) worker.join();
    proto::ListNodesReply list;
    Expect(ListNodes(server.stub(), &list), grpc::OK);
    assert(list.nodes_size() == 120);
}

}  // namespace

int main()
{
    TestRpcContract();
    TestEvictionOverRpc();
    TestConcurrentRpc();
}
