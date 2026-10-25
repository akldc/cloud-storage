#ifndef MYMUDUO_NET_EVENTLOOPTHREAD_H
#define MYMUDUO_NET_EVENTLOOPTHREAD_H

#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace mymuduo {
namespace net {

class EventLoop;

/**
 * @brief 事件循环线程类
 *
 * 该类用于创建一个新线程，并在该线程中运行事件循环。
 * 主要用于IO线程池中，每个IO线程都包含一个EventLoop。
 */
class EventLoopThread {
public:
    // 线程初始化回调函数类型
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    explicit EventLoopThread(
        const ThreadInitCallback& cb = ThreadInitCallback());

    /**
     * @brief 析构函数
     * 如果线程还在运行，会等待线程退出
     */
    ~EventLoopThread();
    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    /**
     * @brief 启动线程并返回其EventLoop对象
     *
     * 该函数会创建新线程并等待EventLoop创建完成。
     * 线程函数会创建EventLoop对象并通知此函数。
     *
     * @return EventLoop* 新创建的EventLoop对象指针
     */
    EventLoop* startLoop();

private:
    /**
     * @brief 线程函数
     *
     * 创建EventLoop对象并运行事件循环
     */
    void threadFunc();

    EventLoop* loop_;        // loop_指针指向一个EventLoop对象（和subLoop是一个概念）
    std::thread thread_;
    std::mutex mutex_;      // 互斥锁
    std::condition_variable cond_;  // 条件变量
    ThreadInitCallback callback_;   // 回调函数
};

} // namespace net
} // namespace mymuduo

#endif // MYMUDUO_NET_EVENTLOOPTHREAD_H
