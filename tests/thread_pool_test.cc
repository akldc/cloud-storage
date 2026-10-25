#include "base/ThreadPool.h"

#include <atomic>
#include <cassert>
#include <thread>

int main()
{
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

    mymuduo::ThreadPool inlinePool("inline-");
    inlinePool.start(0);
    const std::thread::id caller = std::this_thread::get_id();
    std::thread::id executed;
    inlinePool.run([&executed] { executed = std::this_thread::get_id(); });
    inlinePool.stop();
    assert(executed == caller);
}
