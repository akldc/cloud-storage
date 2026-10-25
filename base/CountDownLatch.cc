#include "CountDownLatch.h"

#include <cassert>

namespace mymuduo
{

CountDownLatch::CountDownLatch(int count)
    : mutex_(),
      count_(count)
{
    assert(count >= 0);
}

void CountDownLatch::wait()
{
    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [this] { return count_ == 0; });
}

void CountDownLatch::countDown()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (count_ > 0 && --count_ == 0)
    {
        condition_.notify_all();
    }
}

int CountDownLatch::getCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return count_;
}

} // namespace mymuduo
