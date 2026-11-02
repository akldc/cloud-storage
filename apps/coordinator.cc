#include "coordinator/Config.h"
#include "coordinator/CoordinatorService.h"

#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

namespace {

bool ParsePositiveUint(const char* text, uint32_t* value)
{
    const std::string input(text);
    if (input.empty()) return false;
    auto parsed = std::from_chars(input.data(), input.data() + input.size(), *value);
    return parsed.ec == std::errc{} && parsed.ptr == input.data() + input.size() && *value > 0;
}

}  // namespace

int main(int argc, char* argv[])
{
    using namespace object_storage::coordinator;
    if ((argc != 2 && argc != 5) || argv[1][0] == '\0') {
        std::cerr << "usage: coordinator LISTEN_ADDRESS [HEARTBEAT_MS LEASE_MS RPC_DEADLINE_MS]\n";
        return 2;
    }
    uint32_t heartbeatMs = kDefaultHeartbeatIntervalMs;
    uint32_t leaseMs = kDefaultLeaseTimeoutMs;
    uint32_t rpcDeadlineMs = kDefaultRpcDeadlineMs;
    if (argc == 5 && (!ParsePositiveUint(argv[2], &heartbeatMs) ||
                      !ParsePositiveUint(argv[3], &leaseMs) ||
                      !ParsePositiveUint(argv[4], &rpcDeadlineMs))) {
        std::cerr << "timing values must be positive uint32 milliseconds\n";
        return 2;
    }
    if (!ValidTiming(heartbeatMs, leaseMs, rpcDeadlineMs)) {
        std::cerr << "lease must exceed 3 * heartbeat interval + RPC deadline\n";
        return 2;
    }

    sigset_t signals;
    ::sigemptyset(&signals);
    ::sigaddset(&signals, SIGINT);
    ::sigaddset(&signals, SIGTERM);
    const int maskError = ::pthread_sigmask(SIG_BLOCK, &signals, nullptr);
    if (maskError != 0) {
        std::cerr << "cannot block termination signals: " << maskError << '\n';
        return 1;
    }

    NodeRegistry registry{std::chrono::milliseconds(leaseMs)};
    CoordinatorService service(&registry, heartbeatMs, leaseMs);
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(1024 * 1024);
    builder.SetMaxSendMessageSize(1024 * 1024);
    builder.RegisterService(&service);
    int selectedPort = 0;
    builder.AddListeningPort(argv[1], grpc::InsecureServerCredentials(), &selectedPort);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server || selectedPort == 0) {
        if (server) {
            server->Shutdown(std::chrono::system_clock::now());
            server->Wait();
        }
        std::cerr << "cannot listen on " << argv[1] << '\n';
        return 1;
    }
    std::cout << "coordinator listening on port " << selectedPort << std::endl;

    std::thread stopSignal([&] {
        int signal = 0;
        const int error = ::sigwait(&signals, &signal);
        if (error != 0) std::cerr << "sigwait failed: " << error << '\n';
        server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
    });
    server->Wait();
    stopSignal.join();
}
