#include "storage/node/StorageNode.h"

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <pthread.h>
#include <thread>

int main(int argc, char* argv[])
{
    if (argc != 3) {
        std::cerr << "usage: storage_node ROOT PORT\n";
        return 2;
    }
    char* end = nullptr;
    const unsigned long port = std::strtoul(argv[2], &end, 10);
    if (*argv[2] == '\0' || *end != '\0' || port > 65535) {
        std::cerr << "invalid port\n";
        return 2;
    }
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
    std::thread stopSignal([&] {
        int signal = 0;
        if (::sigwait(&signals, &signal) == 0) loop.quit();
    });
    loop.loop();
    stopSignal.join();
}
