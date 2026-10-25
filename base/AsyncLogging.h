#ifndef MYMUDUO_BASE_ASYNCLOGGING_H
#define MYMUDUO_BASE_ASYNCLOGGING_H

#include "CountDownLatch.h"
#include "LogStream.h"
#include "Types.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

namespace mymuduo
{

/**
 * @brief 异步日志类
 * 
 * 特点：
 * 1. 双缓冲设计
 * 2. 异步写入磁盘
 * 3. 支持多线程
 * 4. 自动滚动日志文件
 */
class AsyncLogging
{
public:
    /**
     * @brief 构造函数
     * @param basename 日志文件基本名
     * @param rollSize 滚动大小
     * @param flushInterval 刷新间隔，秒
     */
    AsyncLogging(const string& basename,
                off_t rollSize,
                int flushInterval = 3);
    AsyncLogging(const AsyncLogging&) = delete;
    AsyncLogging& operator=(const AsyncLogging&) = delete;
    ~AsyncLogging()
    {
        if (running_)
        {
            stop();
        }
    }

    /**
     * @brief 前端调用接口，写入日志
     */
    void append(const char* logline, int len);

    /**
     * @brief 启动日志线程
     */
    void start()
    {
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true)) {
            return;
        }
        thread_ = std::thread(&AsyncLogging::threadFunc, this);
        latch_.wait();
    }

    /**
     * @brief 停止日志线程
     */
    void stop()
    {
        if (!running_.exchange(false)) {
            return;
        }
        cond_.notify_one();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

private:
    /**
     * @brief 后端线程函数
     */
    void threadFunc();

    typedef mymuduo::detail::FixedBuffer<mymuduo::detail::kLargeBuffer> Buffer;
    typedef std::vector<std::unique_ptr<Buffer>> BufferVector;
    typedef BufferVector::value_type BufferPtr;

    const int flushInterval_;        // 刷新间隔
    std::atomic<bool> running_;      // 运行标志
    const string basename_;          // 日志文件基本名
    const off_t rollSize_;          // 滚动大小
    std::thread thread_;
    mymuduo::CountDownLatch latch_; // 用于等待线程启动
    std::mutex mutex_;
    std::condition_variable cond_;
    BufferPtr currentBuffer_;       // 当前缓冲区
    BufferPtr nextBuffer_;          // 预备缓冲区
    BufferVector buffers_;          // 待写入文件的缓冲区列表
};

} // namespace mymuduo

#endif // MYMUDUO_BASE_ASYNCLOGGING_H
