#include "ThreadPool.h"
#include "Exception.h"

#include <assert.h>
#include <stdio.h>

using namespace mymuduo;

ThreadPool::ThreadPool(const string& nameArg)
    : name_(nameArg),
      mutex_(),
      notEmpty_(),
      notFull_(),
      running_(false),
      maxQueueSize_(0)
{
}

ThreadPool::~ThreadPool()
{
    stop();
}

void ThreadPool::start(int numThreads)
{
    assert(numThreads >= 0);
    std::lock_guard<std::mutex> lock(mutex_);
    assert(!running_);
    assert(threads_.empty());
    running_ = true;
    threads_.reserve(static_cast<size_t>(numThreads));
    for (int i = 0; i < numThreads; ++i)
    {
        threads_.emplace_back(&ThreadPool::runInThread, this);
    }
}

void ThreadPool::setMaxQueueSize(size_t maxSize)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        maxQueueSize_ = maxSize;
    }
    notFull_.notify_all();
}

void ThreadPool::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ && threads_.empty()) {
            return;
        }
        running_ = false;
    }
    notEmpty_.notify_all();
    notFull_.notify_all();

    for (auto& thr : threads_)
    {
        assert(thr.get_id() != std::this_thread::get_id());
        if (thr.joinable()) {
            thr.join();
        }
    }
    threads_.clear();
}

size_t ThreadPool::queueSize() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

void ThreadPool::run(Task task)
{
    if (!task) {
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    if (!running_) {
        return;
    }

    if (threads_.empty())
    {
        lock.unlock();
        task();
    }
    else
    {
        notFull_.wait(lock, [this] { return !isFull() || !running_; });

        if (!running_) {
            return;
        }

        assert(!isFull());

        queue_.push_back(std::move(task));
        lock.unlock();
        notEmpty_.notify_one();
    }
}

ThreadPool::Task ThreadPool::take()
{
    std::unique_lock<std::mutex> lock(mutex_);
    notEmpty_.wait(lock, [this] { return !queue_.empty() || !running_; });

    Task task;
    if (!queue_.empty())
    {
        task = std::move(queue_.front());
        queue_.pop_front();
        if (maxQueueSize_ > 0)
        {
            lock.unlock();
            notFull_.notify_one();
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
    // mutex_ must be held by the caller.
    return maxQueueSize_ > 0 && queue_.size() >= maxQueueSize_;
}
