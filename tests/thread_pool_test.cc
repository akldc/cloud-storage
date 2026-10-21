#include "base/ThreadPool.h"

#include <atomic>
#include <cassert>

int main()
{
    mymuduo::ThreadPool pool("regression-");
    pool.start(4);
    std::atomic<int> completed{0};
    for (int i = 0; i < 1000; ++i) {
        pool.run([&completed]() { ++completed; });
    }
    pool.stop();
    assert(completed.load() == 1000);
}
