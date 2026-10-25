#include "base/CountDownLatch.h"

#include <atomic>
#include <cassert>
#include <thread>
#include <vector>

int main()
{
    constexpr int kThreadCount = 8;
    mymuduo::CountDownLatch ready(kThreadCount);
    mymuduo::CountDownLatch release(1);
    std::atomic<int> completed{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&] {
            ready.countDown();
            release.wait();
            ++completed;
        });
    }

    ready.wait();
    assert(ready.getCount() == 0);
    assert(completed.load() == 0);
    release.countDown();

    for (auto& thread : threads) {
        thread.join();
    }
    assert(completed.load() == kThreadCount);
}
