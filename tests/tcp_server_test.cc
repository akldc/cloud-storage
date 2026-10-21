#include "base/ThreadPool.h"
#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/TcpConnection.h"
#include "net/TcpServer.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using namespace mymuduo;
using namespace mymuduo::net;

namespace {

bool runClient(uint16_t port, int id)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof address) < 0) {
        ::close(fd);
        return false;
    }

    const std::string request = "client-" + std::to_string(id) + "-" +
                                std::string(8192 + id, static_cast<char>('a' + id % 26));
    size_t sent = 0;
    while (sent < request.size()) {
        ssize_t n = ::send(fd, request.data() + sent, request.size() - sent, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { ::close(fd); return false; }
        sent += static_cast<size_t>(n);
    }

    std::string response(request.size(), '\0');
    size_t received = 0;
    while (received < response.size()) {
        ssize_t n = ::recv(fd, &response[received], response.size() - received, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { ::close(fd); return false; }
        received += static_cast<size_t>(n);
    }
    ::shutdown(fd, SHUT_RDWR);
    ::close(fd);
    return response == request;
}

}  // namespace

int main()
{
    constexpr int kClients = 24;
    EventLoop loop;
    ThreadPool workers("echo-worker-");
    workers.start(3);

    TcpServer server(&loop, InetAddress(0, true), "RegressionServer");
    server.setThreadNum(2);
    std::atomic<int> connected{0};
    std::atomic<int> disconnected{0};
    std::atomic<int> failures{0};

    server.setConnectionCallback([&](const TcpConnectionPtr& conn) {
        if (conn->connected()) {
            ++connected;
        } else if (++disconnected == kClients) {
            loop.quit();
        }
    });
    server.setMessageCallback([&workers](const TcpConnectionPtr& conn,
                                         Buffer* buffer, Timestamp) {
        std::string message = buffer->retrieveAllAsString();
        workers.run([conn, message = std::move(message)]() {
            conn->send(message.data(), static_cast<int>(message.size()));
        });
    });
    server.start();
    const uint16_t port = server.port();
    assert(port != 0);

    std::thread clients([port, &failures]() {
        std::vector<std::thread> threads;
        for (int i = 0; i < kClients; ++i) {
            threads.emplace_back([port, i, &failures]() {
                if (!runClient(port, i)) ++failures;
            });
        }
        for (std::thread& thread : threads) thread.join();
    });

    loop.runAfter(8.0, [&loop, &failures]() {
        ++failures;
        loop.quit();
    });
    loop.loop();
    clients.join();
    workers.stop();

    assert(failures.load() == 0);
    assert(connected.load() == kClients);
    assert(disconnected.load() == kClients);
}
