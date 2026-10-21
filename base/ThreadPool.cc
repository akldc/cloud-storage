#include "ThreadPool.h"
#include "Exception.h"

#include <assert.h>
#include <stdio.h>

using namespace mymuduo;

ThreadPool::ThreadPool(const string& nameArg)
    : name_(nameArg),
      mutex_(),
      notEmpty_(mutex_),
      notFull_(mutex_),
      running_(false),
      maxQueueSize_(0),
      threadSize_(0)
{
}

ThreadPool::~ThreadPool()
{
    if (running_)
    {
        stop();
    }
}

void ThreadPool::start(int numThreads)
{
    assert(!running_);
    assert(numThreads >= 0);
    running_ = true;
    threadSize_ = numThreads;
    // 创建线程
    threads_.reserve(static_cast<size_t>(threadSize_));
    for (int i = 0; i < threadSize_; ++i)
    {
        char id[32];
        snprintf(id, sizeof id, "%d", i+1);
        threads_.emplace_back(new Thread(
            std::bind(&ThreadPool::runInThread, this),
            name_+id
        ));
        threads_[static_cast<size_t>(i)]->start();
    }

}

void ThreadPool::stop()
{
    {
        MutexLockGuard lock(mutex_);
        running_ = false;
        notEmpty_.notifyAll();
        notFull_.notifyAll();
    }

    for (auto& thr : threads_)
    {
        thr->join();
    }
}

size_t ThreadPool::queueSize() const
{
    MutexLockGuard lock(mutex_);
    return queue_.size();
}

void ThreadPool::run(Task task)
{
    if (threads_.empty())
    {
        bool runInline = false;
        {
            MutexLockGuard lock(mutex_);
            runInline = running_;
        }
        if (runInline && task)
        {
            task();
        }
    }
    else
    {
        // 加锁，将任务添加到任务队列
        MutexLockGuard lock(mutex_);
        while (isFull() && running_)
        {
            notFull_.wait();
        }

        if (!running_) return;

        assert(!isFull());

        queue_.push_back(std::move(task));
        notEmpty_.notify();
    }
}

ThreadPool::Task ThreadPool::take()
{
    MutexLockGuard lock(mutex_);
    // 等待任务队列非空
    while (queue_.empty() && running_)
    {
        notEmpty_.wait();
    }

    Task task;
    if (!queue_.empty())
    {
        task = queue_.front();
        queue_.pop_front();
        if (maxQueueSize_ > 0)
        {
            notFull_.notify();
        }
    }

    return task;
}

void ThreadPool::runInThread()
{
    try
    {
        while (true)
        {
            Task task(take());
            if (!task)
            {
                break;
            }
            task();
        }
    }
    catch (const Exception& ex)
    {
        fprintf(stderr, "exception caught in ThreadPool %s\n", name_.c_str());
        fprintf(stderr, "reason: %s\n", ex.what());
        fprintf(stderr, "stack trace: %s\n", ex.stackTrace());
        abort();
    }
    catch (const std::exception& ex)
    {
        fprintf(stderr, "exception caught in ThreadPool %s\n", name_.c_str());
        fprintf(stderr, "reason: %s\n", ex.what());
        abort();
    }
    catch (...)
    {
        fprintf(stderr, "unknown exception caught in ThreadPool %s\n", name_.c_str());
        throw; // rethrow
    }
}

bool ThreadPool::isFull() const
{
    return maxQueueSize_ > 0 && queue_.size() >= maxQueueSize_;
}
