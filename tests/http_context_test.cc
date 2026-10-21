#include "net/Buffer.h"
#include "net/HttpContext.h"
#include "net/HttpResponse.h"

#include <cassert>

using namespace mymuduo;
using namespace mymuduo::net;

int main()
{
    HttpContext context;
    Buffer input;
    input.append("POST /objects?id=7 HTTP/1.1\r\nhost: localhost\r\nContent-Length: 5\r\n\r\nhe");
    assert(context.parseRequest(&input, Timestamp::now()) == HttpContext::kHeadersComplete);
    input.append("llo");
    assert(context.parseRequest(&input, Timestamp::now()) == HttpContext::kGotRequest);
    assert(context.request().path() == "/objects");
    assert(context.request().getHeader("Host") == "localhost");
    assert(context.request().body() == "hello");

    context.reset();
    input.append("POST / HTTP/1.1\r\nContent-Length: -1\r\n\r\n");
    assert(context.parseRequest(&input, Timestamp::now()) == HttpContext::kError);

    context.reset();
    input.retrieveAll();
    input.append("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n");
    assert(context.parseRequest(&input, Timestamp::now()) == HttpContext::kError);

    HttpResponse response(false);
    response.setStatusCode(HttpResponse::k200Ok);
    response.setStatusMessage("OK");
    response.setBody("data");
    Buffer output;
    response.appendToBuffer(&output);
    const std::string wire = output.retrieveAllAsString();
    assert(wire.find("Content-Length: 4\r\n") != std::string::npos);
}
