#include "storage/node/StorageNode.h"
#ifdef MYMUDUO_BUILD_COORDINATOR
#include "storage/node/CoordinatorAgent.h"
#include "storage/common/Types.h"
#endif

#include <charconv>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <pthread.h>
#include <string>
#include <thread>
#include <utility>

#ifdef MYMUDUO_BUILD_COORDINATOR
namespace {

bool ParsePositiveUint(const char* text, uint32_t* value)
{
    const std::string input(text);
    if (input.empty()) return false;
    auto parsed = std::from_chars(input.data(), input.data() + input.size(), *value);
    return parsed.ec == std::errc{} && parsed.ptr == input.data() + input.size() && *value > 0;
}

}  // namespace
#endif

int main(int argc, char* argv[])
{
#ifdef MYMUDUO_BUILD_COORDINATOR
    if (argc != 3 && argc != 6 && argc != 8) {
        std::cerr << "usage: storage_node ROOT PORT [COORDINATOR_ADDRESS NODE_ID "
                     "ADVERTISED_HOST [RPC_DEADLINE_MS CONFLICT_WAIT_MS]]\n";
        return 2;
    }
#else
    if (argc != 3) {
        std::cerr << "usage: storage_node ROOT PORT\n";
        return 2;
    }
#endif
    char* end = nullptr;
    const unsigned long port = std::strtoul(argv[2], &end, 10);
    if (*argv[2] == '\0' || *end != '\0' || port > 65535) {
        std::cerr << "invalid port\n";
        return 2;
    }
#ifdef MYMUDUO_BUILD_COORDINATOR
    const bool useCoordinator = argc != 3;
    object_storage::CoordinatorAgent::Options agentOptions;
    if (useCoordinator) {
        agentOptions.address = argv[3];
        agentOptions.nodeId = argv[4];
        agentOptions.advertisedHost = argv[5];
        if (agentOptions.address.empty() || !object_storage::IsCanonicalId(agentOptions.nodeId) ||
            agentOptions.advertisedHost.empty() || agentOptions.advertisedHost.size() > 253 ||
            agentOptions.advertisedHost == "0.0.0.0" || agentOptions.advertisedHost == "::") {
            std::cerr << "invalid Coordinator address, node ID, or advertised host\n";
            return 2;
        }
        if (argc == 8 && (!ParsePositiveUint(argv[6], &agentOptions.rpcDeadlineMs) ||
                          !ParsePositiveUint(argv[7], &agentOptions.conflictWaitMs))) {
            std::cerr << "invalid Coordinator timing values\n";
            return 2;
        }
        if (agentOptions.rpcDeadlineMs > object_storage::coordinator::kMaxRpcDeadlineMs ||
            agentOptions.conflictWaitMs < agentOptions.rpcDeadlineMs ||
            agentOptions.conflictWaitMs > object_storage::coordinator::kMaxConflictWaitMs) {
            std::cerr << "Coordinator timing values out of range\n";
            return 2;
        }
    }
#endif
    sigset_t signals;
    ::sigemptyset(&signals);
    ::sigaddset(&signals, SIGINT);
    ::sigaddset(&signals, SIGTERM);
    if (::pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) return 1;

    mymuduo::net::EventLoop loop;
    object_storage::StorageNode::Options options;
    if (const char* delay = std::getenv("STORAGE_NODE_DISK_DELAY_MS")) {
        char* delayEnd = nullptr;
        const unsigned long value = std::strtoul(delay, &delayEnd, 10);
        if (*delay == '\0' || *delayEnd != '\0' || value > 2000) {
            std::cerr << "invalid STORAGE_NODE_DISK_DELAY_MS\n";
            return 2;
        }
        options.diskDelayMs = static_cast<unsigned>(value);
    }
    if (const char* timeout = std::getenv("STORAGE_NODE_IDLE_TIMEOUT_SEC")) {
        char* timeoutEnd = nullptr;
        const unsigned long value = std::strtoul(timeout, &timeoutEnd, 10);
        if (*timeout == '\0' || *timeoutEnd != '\0' || value == 0 || value > 3600) {
            std::cerr << "invalid STORAGE_NODE_IDLE_TIMEOUT_SEC\n";
            return 2;
        }
        options.idleTimeoutSeconds = static_cast<unsigned>(value);
    }
    object_storage::StorageNode node(
        &loop, mymuduo::net::InetAddress(static_cast<uint16_t>(port)), argv[1], options);
    auto started = node.Start();
    if (!started.ok()) {
        std::cerr << started.message() << '\n';
        return 1;
    }
    std::cout << "storage_node listening on " << node.port() << std::endl;
#ifdef MYMUDUO_BUILD_COORDINATOR
    std::optional<object_storage::CoordinatorAgent> agent;
    if (useCoordinator) {
        agentOptions.advertisedPort = node.port();
        try {
            agent.emplace(std::move(agentOptions));
            agent->Start();
        } catch (const std::exception& error) {
            std::cerr << "cannot start Coordinator agent: " << error.what() << '\n';
            return 1;
        }
    }
#endif
    std::thread stopSignal([&] {
        int signal = 0;
        if (::sigwait(&signals, &signal) == 0) loop.quit();
    });
    loop.loop();
    stopSignal.join();
#ifdef MYMUDUO_BUILD_COORDINATOR
    if (agent) agent->Stop();
#endif
}
