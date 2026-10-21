#include "net/EventLoop.h"
#include "net/EventLoopThread.h"

#include <atomic>
#include <cassert>
#include <future>
#include <thread>

int main()
{
    mymuduo::net::EventLoopThread loopThread;
    mymuduo::net::EventLoop* loop = loopThread.startLoop();
    std::promise<void> done;
    std::future<void> future = done.get_future();
    const std::thread::id caller = std::this_thread::get_id();

    loop->queueInLoop([&done, caller, loop]() {
        assert(std::this_thread::get_id() != caller);
        done.set_value();
        loop->quit();
    });
    assert(future.wait_for(std::chrono::seconds(2)) == std::future_status::ready);
}
