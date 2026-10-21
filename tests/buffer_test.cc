#include "net/Buffer.h"

#include <cassert>
#include <string>
#include <unistd.h>

using mymuduo::net::Buffer;

int main()
{
    Buffer buffer(8);
    buffer.append("abcdef", 6);
    assert(buffer.retrieveAsString(4) == "abcd");
    buffer.append("ghijkl", 6);  // exercises compaction/growth
    assert(buffer.retrieveAllAsString() == "efghijkl");

    int fds[2];
    assert(::pipe(fds) == 0);
    const std::string payload(70000, 'x');
    // Keep the pipe test below its typical capacity to avoid blocking.
    const std::string small = payload.substr(0, 4096);
    assert(::write(fds[1], small.data(), small.size()) ==
           static_cast<ssize_t>(small.size()));
    int savedErrno = 0;
    assert(buffer.readFd(fds[0], &savedErrno) == static_cast<ssize_t>(small.size()));
    assert(buffer.retrieveAllAsString() == small);
    ::close(fds[0]);
    ::close(fds[1]);
}
