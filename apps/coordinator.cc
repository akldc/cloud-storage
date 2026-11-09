#include "coordinator/Config.h"
#include "coordinator/CoordinatorService.h"
#ifdef MYMUDUO_BUILD_OBJECTS
#include "coordinator/ObjectCatalog.h"
#endif

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
    const bool hasDb = (argc == 4 || argc == 7) &&
                       std::string(argv[argc - 2]) == "--db";
    const bool hasTimings = argc == 5 || argc == 7;
    if ((argc != 2 && argc != 5 && !hasDb) || argv[1][0] == '\0') {
        std::cerr << "usage: coordinator LISTEN_ADDRESS [HEARTBEAT_MS LEASE_MS RPC_DEADLINE_MS]"
#ifdef MYMUDUO_BUILD_OBJECTS
                  << " [--db SQLITE_PATH]"
#endif
                  << '\n';
        return 2;
    }
#ifndef MYMUDUO_BUILD_OBJECTS
    if (hasDb) {
        std::cerr << "object Catalog support is disabled in this build\n";
        return 2;
    }
#endif
    uint32_t heartbeatMs = kDefaultHeartbeatIntervalMs;
    uint32_t leaseMs = kDefaultLeaseTimeoutMs;
    uint32_t rpcDeadlineMs = kDefaultRpcDeadlineMs;
    if (hasTimings && (!ParsePositiveUint(argv[2], &heartbeatMs) ||
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
#ifdef MYMUDUO_BUILD_OBJECTS
    std::unique_ptr<ObjectCatalog> catalog;
    if (hasDb) {
        try {
            catalog = std::make_unique<ObjectCatalog>(argv[argc - 1]);
        } catch (const std::exception& error) {
            std::cerr << "cannot open object Catalog: " << error.what() << '\n';
            return 1;
        }
    }
    CoordinatorService service(&registry, heartbeatMs, leaseMs, catalog.get(), rpcDeadlineMs);
#else
    CoordinatorService service(&registry, heartbeatMs, leaseMs);
#endif
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
