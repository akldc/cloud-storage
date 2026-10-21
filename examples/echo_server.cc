#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "net/InetAddress.h"
#include "net/TcpConnection.h"
#include "net/TcpServer.h"

#include <cstdlib>
#include <iostream>

using mymuduo::Timestamp;
using namespace mymuduo::net;

int main(int argc, char* argv[])
{
    const int port = argc > 1 ? std::atoi(argv[1]) : 2007;
    if (port <= 0 || port > 65535) {
        std::cerr << "usage: echo_server [port]\n";
        return 2;
    }

    EventLoop loop;
    TcpServer server(&loop, InetAddress(static_cast<uint16_t>(port)), "EchoServer");
    server.setThreadNum(2);
    server.setMessageCallback(
        [](const TcpConnectionPtr& conn, Buffer* buffer, Timestamp) {
            conn->send(buffer);
        });
    server.start();
    std::cout << "echo server listening on port " << server.port() << std::endl;
    loop.loop();
}
