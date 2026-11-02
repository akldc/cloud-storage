#include "coordinator/NodeRegistry.h"
#include "coordinator/Placement.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace object_storage::coordinator;
using namespace std::chrono_literals;

namespace {

const auto t0 = Clock::time_point{};
const std::string instanceA = "aaaaaaaa-0000-0000-0000-000000000000";
const std::string instanceB = "bbbbbbbb-0000-0000-0000-000000000000";
const std::string chunk = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

std::string Id(unsigned n)
{
    char value[37];
    std::snprintf(value, sizeof value, "%08x-0000-0000-0000-000000000000", n);
    return value;
}

void Expect(Status status, Code code) { assert(status.code == code); }

void TestIdentityLeaseAndEndpoints()
{
    NodeRegistry registry(10s);
    Expect(registry.Register(Id(1), instanceA, "NODE.Example", 8000, t0), Code::kOk);
    auto first = registry.GetNode(Id(1), t0);
    assert(first.ok() && first.value->online && first.value->host == "node.example");
    const auto seen = first.value->lastSeenUnixMs;

    // Same request after a lost response is idempotent and refreshes the lease.
    Expect(registry.Register(Id(1), instanceA, "node.example", 8000, t0 + 5s), Code::kOk);
    assert(registry.GetNode(Id(1), t0 + 14s).value->online);
    Expect(registry.Register(Id(1), instanceA, "other.example", 8000, t0 + 6s),
           Code::kFailedPrecondition);
    assert(registry.GetNode(Id(1), t0 + 6s).value->host == "node.example");
    Expect(registry.Register(Id(1), instanceB, "node.example", 8000, t0 + 6s),
           Code::kAlreadyExists);
    Expect(registry.Register(Id(2), instanceA, "NODE.EXAMPLE", 8000, t0 + 6s),
           Code::kAlreadyExists);
    Expect(registry.Register(Id(2), instanceA, "node.example", 8001, t0 + 6s), Code::kOk);
    Expect(registry.Heartbeat(Id(1), instanceA, t0 + 15s), Code::kNotFound);
    assert(!registry.GetNode(Id(1), t0 + 15s).value->online);
    Expect(registry.Register(Id(1), instanceB, "new.example", 8002, t0 + 15s), Code::kOk);
    Expect(registry.Heartbeat(Id(1), instanceA, t0 + 16s), Code::kFailedPrecondition);
    Expect(registry.Unregister(Id(1), instanceA), Code::kFailedPrecondition);
    assert(registry.GetNode(Id(1), t0 + 16s).value->instanceId == instanceB);
    Expect(registry.Unregister(Id(1), instanceB), Code::kOk);
    Expect(registry.Unregister(Id(1), instanceB), Code::kOk);
    Expect(registry.Heartbeat(Id(1), instanceB, t0 + 16s), Code::kNotFound);
    Expect(registry.Register(Id(3), instanceA, "new.example", 8002, t0 + 16s), Code::kOk);
    assert(registry.GetNode(Id(1), t0 + 16s).value->lastSeenUnixMs >= seen);
    assert(registry.OnlineNodes(t0 + 16s).size() == 1);
    assert(registry.ListNodes(t0 + 16s).size() == 3);

    // An expired matching instance must register again and recheck its endpoint.
    Expect(registry.Register(Id(2), instanceA, "node.example", 8001, t0 + 16s), Code::kOk);
    Expect(registry.Heartbeat(Id(2), instanceA, t0 + 26s), Code::kNotFound);
    Expect(registry.Register(Id(4), instanceA, "node.example", 8001, t0 + 26s), Code::kOk);
    Expect(registry.Register(Id(2), instanceA, "node.example", 8001, t0 + 26s),
           Code::kAlreadyExists);
    Expect(registry.Register(Id(2), instanceB, "fresh.example", 8001, t0 + 26s), Code::kOk);
}

void TestValidationAndHostNormalization()
{
    NodeRegistry registry;
    Expect(registry.Register("", instanceA, "host", 1, t0), Code::kInvalidArgument);
    Expect(registry.Register(Id(1), "AAAAAAAA-0000-0000-0000-000000000000", "host", 1, t0),
           Code::kInvalidArgument);
    for (const auto& host : {"", "0.0.0.0", "::", "bad_host", "-bad.example",
                             "bad..example", "bad.example-", "fe80::gggg"}) {
        Expect(registry.Register(Id(1), instanceA, host, 1, t0), Code::kInvalidArgument);
    }
    Expect(registry.Register(Id(1), instanceA, "host", 0, t0), Code::kInvalidArgument);
    Expect(registry.Register(Id(1), instanceA, "host", 65536, t0), Code::kInvalidArgument);
    Expect(registry.Register(Id(1), instanceA, "2001:0DB8:0:0::1", 1, t0), Code::kOk);
    assert(registry.GetNode(Id(1), t0).value->host == "2001:db8::1");
    Expect(registry.Register(Id(2), instanceA, "2001:db8::1", 1, t0), Code::kAlreadyExists);
    Expect(registry.Heartbeat(Id(3), instanceA, t0), Code::kNotFound);
    Expect(registry.Unregister(Id(3), instanceA), Code::kNotFound);
    assert(registry.GetNode(Id(3), t0).status.code == Code::kNotFound);
    assert(registry.GetNode("", t0).status.code == Code::kInvalidArgument);
    Expect(registry.Heartbeat(Id(1), "", t0), Code::kInvalidArgument);
}

void TestCapacityAndEviction()
{
    NodeRegistry registry(10s);
    for (unsigned i = 1; i <= kMaxNodes; ++i) {
        Expect(registry.Register(Id(i), instanceA, "host" + std::to_string(i), 8000, t0),
               Code::kOk);
    }
    assert(registry.ListNodes(t0).size() == kMaxNodes);
    Expect(registry.Register(Id(2000), instanceA, "new", 8000, t0 + 1s),
           Code::kResourceExhausted);
    assert(registry.GetNode(Id(1), t0 + 1s).ok());
    Expect(registry.Register(Id(2000), instanceA, "HOST2", 8000, t0 + 1s),
           Code::kAlreadyExists);
    Expect(registry.Unregister(Id(3), instanceA), Code::kOk);
    Expect(registry.Unregister(Id(4), instanceA), Code::kOk);
    Expect(registry.Register(Id(2000), instanceA, "host2", 8000, t0 + 1s),
           Code::kAlreadyExists);
    assert(registry.GetNode(Id(3), t0 + 1s).ok()); // Failed requests do not evict.
    Expect(registry.Register(Id(2000), instanceA, "new", 8000, t0 + 1s), Code::kOk);
    assert(registry.GetNode(Id(3), t0 + 1s).status.code == Code::kNotFound);
    assert(registry.ListNodes(t0 + 1s).size() == kMaxNodes);

    // Unregistered records win over expired ones; equal timestamps use node ID.
    Expect(registry.Register(Id(2001), instanceA, "another", 8000, t0 + 10s), Code::kOk);
    assert(registry.GetNode(Id(4), t0 + 10s).status.code == Code::kNotFound);
    assert(registry.GetNode(Id(1), t0 + 10s).ok());
    // At exact lease expiry, the oldest expired record is evicted by ID.
    Expect(registry.Register(Id(2002), instanceA, "third", 8000, t0 + 10s), Code::kOk);
    assert(registry.GetNode(Id(1), t0 + 10s).status.code == Code::kNotFound);
    assert(registry.GetNode(Id(2), t0 + 10s).ok());
}

void TestPlacement()
{
    NodeRegistry registry;
    for (unsigned i = 1; i <= 4; ++i) {
        Expect(registry.Register(Id(i), instanceA, "host" + std::to_string(i), 8000, t0),
               Code::kOk);
    }
    auto nodes = registry.OnlineNodes(t0);
    nodes.push_back(nodes.front());
    assert(GetPlacement("", 1, nodes).status.code == Code::kInvalidArgument);
    assert(GetPlacement(chunk, 0, nodes).status.code == Code::kInvalidArgument);
    assert(GetPlacement(chunk, 17, nodes).status.code == Code::kInvalidArgument);
    assert(GetPlacement(chunk, 5, nodes).status.code == Code::kFailedPrecondition);

    auto chosen = GetPlacement(chunk, 4, nodes);
    assert(chosen.ok() && chosen.value->size() == 4);
    const std::vector<std::string> expected{Id(2), Id(4), Id(1), Id(3)};
    for (size_t i = 0; i < expected.size(); ++i) assert((*chosen.value)[i].nodeId == expected[i]);
    std::reverse(nodes.begin(), nodes.end());
    auto reversed = GetPlacement(chunk, 4, nodes);
    for (size_t i = 0; i < expected.size(); ++i) assert((*reversed.value)[i].nodeId == expected[i]);
    std::set<std::string> distinct;
    for (const auto& node : *chosen.value) distinct.insert(node.nodeId);
    assert(distinct.size() == 4);

    Expect(registry.Unregister(Id(2), instanceA), Code::kOk);
    auto smaller = GetPlacement(chunk, 3, registry.OnlineNodes(t0));
    assert(smaller.ok() && (*smaller.value)[0].nodeId == Id(4));
    assert(GetPlacement(chunk, 4, registry.OnlineNodes(t0)).status.code ==
           Code::kFailedPrecondition);
    Expect(registry.Register(Id(5), instanceA, "host5", 8000, t0), Code::kOk);
    auto added = GetPlacement(chunk, 4, registry.OnlineNodes(t0));
    assert(added.ok() && added.value->size() == 4);
    assert(GetPlacement(chunk, 1, registry.OnlineNodes(t0 + 10s)).status.code ==
           Code::kFailedPrecondition);
}

void TestConcurrentAccess()
{
    NodeRegistry registry;
    std::vector<std::thread> threads;
    for (unsigned worker = 0; worker < 4; ++worker) {
        threads.emplace_back([&, worker] {
            for (unsigned n = 1; n <= 100; ++n) {
                const unsigned id = worker * 100 + n;
                Expect(registry.Register(Id(id), instanceA, "host" + std::to_string(id), 8000,
                                        t0), Code::kOk);
                Expect(registry.Heartbeat(Id(id), instanceA, t0 + 1s), Code::kOk);
                registry.ListNodes(t0 + 1s);
                registry.OnlineNodes(t0 + 1s);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    assert(registry.OnlineNodes(t0 + 1s).size() == 400);
}

}  // namespace

int main()
{
    TestIdentityLeaseAndEndpoints();
    TestValidationAndHostNormalization();
    TestCapacityAndEviction();
    TestPlacement();
    TestConcurrentAccess();
}
