#ifndef MYMUDUO_BASE_COUNTDOWNLATCH_H
#define MYMUDUO_BASE_COUNTDOWNLATCH_H

#include <condition_variable>
#include <mutex>

namespace mymuduo
{

/**
 * @brief 倒计时门闩类，用于线程同步
 * 
 * 特点：
 * 1. 可以用于确保线程安全启动
 * 2. 可以用于等待多个线程完成某个事件
 * 3. 基于条件变量和互斥锁实现
 */
class CountDownLatch
{
public:
    /**
     * @brief 构造函数
     * @param count 初始计数值
     */
    explicit CountDownLatch(int count);
    CountDownLatch(const CountDownLatch&) = delete;
    CountDownLatch& operator=(const CountDownLatch&) = delete;

    /**
     * @brief 等待计数值变为0
     */
    void wait();

    /**
     * @brief 计数值减1
     */
    void countDown();

    /**
     * @brief 获取当前计数值
     */
    int getCount() const;

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    int count_;               // 计数值
};

} // namespace mymuduo

#endif // MYMUDUO_BASE_COUNTDOWNLATCH_H
