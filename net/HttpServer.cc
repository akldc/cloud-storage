#include "HttpServer.h"
#include "base/Logging.h"

#include <strings.h>

using namespace mymuduo;
using namespace mymuduo::net;

namespace mymuduo {
namespace net {
namespace detail {

bool defaultHttpCallback(const TcpConnectionPtr&, const HttpRequest&, HttpResponse* resp) {
    resp->setStatusCode(HttpResponse::k404NotFound);
    resp->setStatusMessage("Not Found");
    resp->setCloseConnection(true);
    return true;
}

} // namespace detail
} // namespace net
} // namespace mymuduo

HttpServer::HttpServer(EventLoop* loop,
                     const InetAddress& listenAddr,
                     const std::string& name)
    : server_(loop, listenAddr, name),
      httpCallback_(detail::defaultHttpCallback)
{
    server_.setConnectionCallback(
        std::bind(&HttpServer::onConnection, this, std::placeholders::_1));
    server_.setMessageCallback(
        std::bind(&HttpServer::onMessage, this, std::placeholders::_1,
                 std::placeholders::_2, std::placeholders::_3));
}

void HttpServer::onConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) {
        auto context = std::make_shared<HttpContext>();
        conn->setContext(context);
    }
}

void HttpServer::onMessage(const TcpConnectionPtr& conn,
                         Buffer* buf,
                         Timestamp receiveTime) {
    auto context = std::static_pointer_cast<HttpContext>(conn->getContext());

    if(!context){
        LOG_INFO << "context is null";
        return;
    }

    while (buf->readableBytes() > 0) {
        HttpContext::ParseResult result = context->parseRequest(buf, receiveTime);
        if (result == HttpContext::kError) {
            conn->send("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
            conn->shutdown();
            return;
        }

        if (result == HttpContext::kGotRequest) {
            bool syncProcessed = onRequest(conn, context->request());
            if (!syncProcessed) {
                return;
            }
            context->reset();
        } else {
            return;
        }
    }
}

bool HttpServer::onRequest(const TcpConnectionPtr& conn, HttpRequest& req) {
    // LOG_DEBUG << "onRequest start";
    const std::string connection = req.getHeader("Connection");
    bool close = ::strcasecmp(connection.c_str(), "close") == 0 ||
        (req.getVersion() == HttpRequest::kHttp10 &&
         ::strcasecmp(connection.c_str(), "Keep-Alive") != 0);
    HttpResponse response(close);

    // 调用用户的回调函数处理请求
    bool syncProcessed = httpCallback_(conn, req, &response);

    // 如果是同步处理完成，或者不是异步响应，直接发送响应
    if (syncProcessed) {
        Buffer buf;
        response.appendToBuffer(&buf);
        conn->send(&buf);
        if (response.closeConnection()) {
            conn->shutdown();
        }
        LOG_INFO << "Sync request completed";
    } else {
        LOG_INFO << "Async request, waiting for response";
    }

    return syncProcessed;
}
