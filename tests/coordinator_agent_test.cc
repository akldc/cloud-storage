#include "storage/node/CoordinatorAgent.h"

#include "coordinator/NodeRegistry.h"
#include "coordinator_v1.grpc.pb.h"
#include "storage/common/Types.h"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace object_storage;
using namespace std::chrono_literals;
namespace rpc = object_storage::control::v1;

namespace {

const std::string nodeId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

template <typename Predicate>
void WaitUntil(Predicate predicate, std::chrono::milliseconds timeout = 3s)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        assert(std::chrono::steady_clock::now() < deadline);
        std::this_thread::sleep_for(10ms);
    }
}

class FakeService final : public rpc::Coordinator::Service {
public:
    enum Scenario { kLostReply, kHeartbeatConflict, kAlwaysConflict,
                    kHeartbeatNotFound, kConflictThenSuccess };
    explicit FakeService(Scenario scenario) : scenario_(scenario), registry_(200ms) {}

    grpc::Status RegisterNode(grpc::ServerContext*, const rpc::RegisterNodeRequest* request,
                              rpc::RegisterNodeReply* reply) override
    {
        const int call = ++registerCalls;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            registrations.push_back(*request);
        }
        if (scenario_ == kAlwaysConflict) {
            return {grpc::ALREADY_EXISTS, "held by another instance"};
        }
        if (scenario_ == kConflictThenSuccess && call == 1) {
            return {grpc::ALREADY_EXISTS, "old instance still online"};
        }
        auto status = registry_.Register(request->node_id(), request->instance_id(),
                                         request->host(), request->port(),
                                         coordinator::Clock::now());
        assert(status.ok());
        if (scenario_ == kLostReply && call == 1) {
            return {grpc::UNAVAILABLE, "simulated lost response"};
        }
        reply->set_heartbeat_interval_ms(20);
        reply->set_lease_timeout_ms(200);
        return grpc::Status::OK;
    }

    grpc::Status Heartbeat(grpc::ServerContext*, const rpc::HeartbeatRequest* request,
                           rpc::HeartbeatReply*) override
    {
        const int call = ++heartbeatCalls;
        if (scenario_ == kHeartbeatConflict) {
            return {grpc::FAILED_PRECONDITION, "instance replaced"};
        }
        if (scenario_ == kHeartbeatNotFound && call == 1) {
            return {grpc::NOT_FOUND, "registry lost"};
        }
        auto status = registry_.Heartbeat(request->node_id(), request->instance_id(),
                                          coordinator::Clock::now());
        assert(status.ok());
        return grpc::Status::OK;
    }

    grpc::Status UnregisterNode(grpc::ServerContext*, const rpc::UnregisterNodeRequest* request,
                                rpc::UnregisterNodeReply*) override
    {
        ++unregisterCalls;
        auto status = registry_.Unregister(request->node_id(), request->instance_id());
        return status.ok() ? grpc::Status::OK : grpc::Status(grpc::NOT_FOUND, status.message);
    }

    std::vector<rpc::RegisterNodeRequest> Requests() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return registrations;
    }
    coordinator::NodeRegistry& registry() { return registry_; }

    std::atomic<int> registerCalls{0};
    std::atomic<int> heartbeatCalls{0};
    std::atomic<int> unregisterCalls{0};

private:
    Scenario scenario_;
    coordinator::NodeRegistry registry_;
    mutable std::mutex mutex_;
    std::vector<rpc::RegisterNodeRequest> registrations;
};

class ServerFixture {
public:
    explicit ServerFixture(FakeService::Scenario scenario) : service(scenario)
    {
        grpc::ServerBuilder builder;
        builder.RegisterService(&service);
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        server_ = builder.BuildAndStart();
        assert(server_ && port > 0);
        address = "127.0.0.1:" + std::to_string(port);
    }
    ~ServerFixture()
    {
        server_->Shutdown(std::chrono::system_clock::now() + 3s);
        server_->Wait();
    }

    FakeService service;
    std::string address;

private:
    std::unique_ptr<grpc::Server> server_;
};

CoordinatorAgent::Options Options(const std::string& address, uint32_t conflictWaitMs = 1000)
{
    CoordinatorAgent::Options options;
    options.address = address;
    options.nodeId = nodeId;
    options.advertisedHost = "127.0.0.1";
    options.advertisedPort = 9080;
    options.rpcDeadlineMs = 50;
    options.conflictWaitMs = conflictWaitMs;
    return options;
}

void TestLostReplyAndUnregister()
{
    ServerFixture server(FakeService::kLostReply);
    CoordinatorAgent agent(Options(server.address));
    assert(IsCanonicalId(agent.instanceId()));
    agent.Start();
    WaitUntil([&] { return server.service.registerCalls >= 2 &&
                           agent.state() == CoordinatorAgent::State::kRegistered &&
                           server.service.heartbeatCalls >= 1; });
    const auto requests = server.service.Requests();
    assert(requests.size() == 2);
    assert(requests[0].node_id() == requests[1].node_id());
    assert(requests[0].instance_id() == requests[1].instance_id());
    assert(requests[0].host() == requests[1].host());
    assert(requests[0].port() == requests[1].port());
    assert(requests[0].instance_id() == agent.instanceId());
    agent.Stop();
    assert(server.service.unregisterCalls == 1);
    assert(!server.service.registry().GetNode(nodeId, coordinator::Clock::now()).value->online);
}

void TestHeartbeatConflictStopsRetry()
{
    ServerFixture server(FakeService::kHeartbeatConflict);
    CoordinatorAgent agent(Options(server.address));
    agent.Start();
    WaitUntil([&] { return agent.state() == CoordinatorAgent::State::kTerminalConflict; });
    std::this_thread::sleep_for(150ms);
    assert(server.service.registerCalls == 1 && server.service.heartbeatCalls == 1);
    agent.Stop();
    assert(server.service.unregisterCalls == 0);
}

void TestRegistrationConflictWindow()
{
    ServerFixture server(FakeService::kAlwaysConflict);
    CoordinatorAgent agent(Options(server.address, 250));
    agent.Start();
    WaitUntil([&] { return agent.state() == CoordinatorAgent::State::kTerminalConflict; });
    const int attempts = server.service.registerCalls;
    std::this_thread::sleep_for(150ms);
    assert(server.service.registerCalls == attempts && attempts >= 1);
    agent.Stop();
}

void TestHeartbeatNotFoundReregisters()
{
    ServerFixture server(FakeService::kHeartbeatNotFound);
    CoordinatorAgent agent(Options(server.address));
    agent.Start();
    WaitUntil([&] { return server.service.registerCalls >= 2 &&
                           server.service.heartbeatCalls >= 2 &&
                           agent.state() == CoordinatorAgent::State::kRegistered; });
    const auto requests = server.service.Requests();
    assert(requests[0].instance_id() == requests[1].instance_id());
    agent.Stop();
    assert(server.service.unregisterCalls == 1);
}

void TestConflictCanResolveAtWindowBoundary()
{
    ServerFixture server(FakeService::kConflictThenSuccess);
    CoordinatorAgent agent(Options(server.address, 250));
    agent.Start();
    WaitUntil([&] { return server.service.registerCalls >= 2 &&
                           agent.state() == CoordinatorAgent::State::kRegistered; });
    agent.Stop();
    assert(server.service.unregisterCalls == 1);
}

void TestStopDuringCoordinatorOutage()
{
    std::unique_ptr<CoordinatorAgent> agent;
    {
        ServerFixture server(FakeService::kHeartbeatNotFound);
        agent = std::make_unique<CoordinatorAgent>(Options(server.address));
        agent->Start();
        WaitUntil([&] { return agent->state() == CoordinatorAgent::State::kRegistered; });
    }
    const auto before = std::chrono::steady_clock::now();
    agent->Stop();
    assert(std::chrono::steady_clock::now() - before < 1s);
}

}  // namespace

int main()
{
    TestLostReplyAndUnregister();
    TestHeartbeatConflictStopsRetry();
    TestRegistrationConflictWindow();
    TestHeartbeatNotFoundReregisters();
    TestConflictCanResolveAtWindowBoundary();
    TestStopDuringCoordinatorOutage();
}
