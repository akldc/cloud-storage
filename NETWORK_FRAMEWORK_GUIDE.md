# 网络框架代码学习指南

这份文档用于快速建立当前网络库的代码地图。建议先理解数据和控制如何沿调用链流动，再逐个阅读类的实现；不要一开始陷入日志、时间格式化或 HTTP 业务细节。

## 1. 一页心智模型

整个框架采用 Reactor + one-loop-per-thread：

```text
应用回调
  ↑
TcpServer / TcpConnection / HttpServer
  ↑
Acceptor / Socket / Buffer
  ↑
EventLoop ── Channel ── Poller/EPollPoller
  │
  ├── eventfd：跨线程唤醒
  └── timerfd：定时器唤醒

EventLoopThreadPool：创建多个 I/O 线程，每个线程一个 EventLoop
base::ThreadPool：执行普通后台任务，不负责网络事件循环
```

最重要的三个概念：

1. **EventLoop 是线程边界**：一个 `EventLoop` 固定属于创建它的线程。
2. **Channel 是 fd 的事件分发器**：它不拥有 fd，只记录关心的事件和回调。
3. **TcpConnection 是连接状态的所有者**：它拥有 socket、Channel、输入输出 Buffer，并通过 `shared_ptr` 管理异步生命周期。

一个 TCP 数据包进入服务端后的主路径：

```text
epoll_wait
  -> EPollPoller::fillActiveChannels
  -> EventLoop::loop
  -> Channel::handleEvent
  -> TcpConnection::handleRead
  -> Buffer::readFd
  -> MessageCallback
  -> TcpConnection::send
  -> sendInLoop / outputBuffer
  -> Channel 开启 POLLOUT
  -> TcpConnection::handleWrite
```

## 2. 目录层级和依赖关系

| 层级 | 主要文件 | 职责 |
| --- | --- | --- |
| 基础设施 | [`base/`](base/) | 线程、锁、条件变量、时间、日志、普通任务线程池 |
| 系统调用封装 | [`Socket.*`](net/Socket.h)、[`SocketsOps.*`](net/SocketsOps.h)、[`InetAddress.*`](net/InetAddress.h) | fd 生命周期、非阻塞 socket、地址转换 |
| Reactor 核心 | [`EventLoop.*`](net/EventLoop.h)、[`Channel.*`](net/Channel.h)、[`Poller.*`](net/Poller.h)、[`poller/`](net/poller/) | 等待、登记和分发 I/O 事件 |
| 定时与跨线程 | [`TimerQueue.*`](net/TimerQueue.h)、[`EventLoopThread.*`](net/EventLoopThread.h)、[`EventLoopThreadPool.*`](net/EventLoopThreadPool.h) | timerfd、eventfd、I/O 线程池 |
| TCP 抽象 | [`Acceptor.*`](net/Acceptor.h)、[`TcpServer.*`](net/TcpServer.h)、[`TcpConnection.*`](net/TcpConnection.h) | 监听、连接建立、收发、关闭和连接表 |
| TCP 客户端 | [`Connector.*`](net/Connector.h)、[`TcpClient.*`](net/TcpClient.h) | 非阻塞 connect、重试和客户端连接管理 |
| HTTP 适配 | [`HttpContext.*`](net/HttpContext.h)、[`HttpServer.*`](net/HttpServer.h)、[`HttpRequest.h`](net/HttpRequest.h)、[`HttpResponse.h`](net/HttpResponse.h) | HTTP 解析、请求模型和响应序列化 |
| 示例与测试 | [`examples/echo_server.cc`](examples/echo_server.cc)、[`tests/`](tests/) | 最小用法和调用链验证 |

CMake 目标关系：

```text
mymuduo::base
    ↑
mymuduo::net
    ↑
mymuduo::http   （可选）
```

`mymuduo::net` 不依赖 HTTP；`mymuduo::http` 是 TCP 层之上的协议适配。

## 3. 先认识公共回调接口

公共回调集中在 [`net/Callbacks.h`](net/Callbacks.h)：

```cpp
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
using ConnectionCallback =
    std::function<void(const TcpConnectionPtr&)>;
using MessageCallback =
    std::function<void(const TcpConnectionPtr&, Buffer*, Timestamp)>;
using WriteCompleteCallback =
    std::function<void(const TcpConnectionPtr&)>;
using HighWaterMarkCallback =
    std::function<void(const TcpConnectionPtr&, size_t)>;
using TimerCallback = std::function<void()>;
```

这些回调构成上层业务与网络框架的主要边界：

- `ConnectionCallback`：连接建立和断开时都会调用，通过 `conn->connected()` 区分。
- `MessageCallback`：输入 Buffer 中出现数据时调用；业务必须消费已经处理的数据。
- `WriteCompleteCallback`：输出 Buffer 清空后调用。
- `HighWaterMarkCallback`：输出积压第一次越过阈值时调用，可用于背压告警。
- `TimerCallback`：由所属 EventLoop 线程执行。

回调通常运行在连接所属 I/O Loop 线程，而不是固定在主线程。

## 4. base：线程、同步与日志

### 4.1 标准库线程与同步

项目在使用点直接采用 `std::thread`、`std::mutex`、`std::lock_guard`、`std::unique_lock` 和 `std::condition_variable`。主要入口是 [`base/ThreadPool.cc`](base/ThreadPool.cc)、[`net/EventLoopThread.cc`](net/EventLoopThread.cc) 和 [`base/AsyncLogging.cc`](base/AsyncLogging.cc)。

[`base/CountDownLatch.h`](base/CountDownLatch.h) 保留为有明确业务语义的小型同步工具，内部也已基于 C++17 标准库实现。阅读时重点关注锁的作用域、条件谓词以及哪些数据由同一把锁保护。

### 4.3 两种“线程池”不要混淆

[`base/ThreadPool.h`](base/ThreadPool.h) 是普通任务线程池：

```cpp
std::deque<Task> queue_;
std::vector<std::thread> threads_;
std::condition_variable notEmpty_;
std::condition_variable notFull_;
```

适合磁盘 I/O、校验等阻塞任务。它的线程没有 EventLoop。

[`net/EventLoopThreadPool.h`](net/EventLoopThreadPool.h) 是 I/O Loop 线程池。每个线程内部运行一个 `EventLoop`，用于处理网络 fd。Storage Node 后续会同时使用这两类线程池，但职责不能互换。

### 4.4 日志

从 [`base/Logging.h`](base/Logging.h) 认识 `LOG_TRACE`、`LOG_INFO`、`LOG_SYSERR` 等宏即可。异步落盘实现位于 [`base/AsyncLogging.cc`](base/AsyncLogging.cc)，初次学习 Reactor 时可以暂时跳过。

## 5. Buffer：网络字节容器

入口文件：[`net/Buffer.h`](net/Buffer.h)、[`net/Buffer.cc`](net/Buffer.cc)。

内存布局：

```text
0                  readerIndex       writerIndex          size
+------------------+-----------------+--------------------+
| prependable      | readable        | writable           |
+------------------+-----------------+--------------------+
```

关键接口：

| 接口 | 含义 |
| --- | --- |
| `peek()` | 可读区起始地址 |
| `readableBytes()` | 当前可读字节数 |
| `retrieve(n)` | 消费 n 字节 |
| `append(data, n)` | 追加数据，必要时整理或扩容 |
| `findCRLF()` | HTTP 行解析使用 |
| `readFd(fd, errno)` | 使用 `readv` 同时读取内部空间和 64 KiB 栈缓冲 |

理解 `makeSpace()` 是阅读 Buffer 的关键：空间不足时，要么扩大 vector，要么把未消费数据移动到 `kCheapPrepend` 后面。

重要约束：一个连接的输入/输出 Buffer 应只在其 I/O Loop 线程访问。Buffer 是网络滑动窗口，不应保存完整大文件。

对应测试：[`tests/buffer_test.cc`](tests/buffer_test.cc)。

## 6. Channel：fd 与回调的绑定

入口文件：[`net/Channel.h`](net/Channel.h)、[`net/Channel.cc`](net/Channel.cc)。

核心结构：

```cpp
EventLoop* loop_;       // 固定所属 EventLoop
const int fd_;          // 观察的 fd，但不拥有它
int events_;            // 希望监听的事件
int revents_;           // Poller 返回的实际事件
int index_;             // 在 Poller 中的状态

ReadEventCallback readCallback_;
EventCallback writeCallback_;
EventCallback closeCallback_;
EventCallback errorCallback_;

std::weak_ptr<void> tie_;
```

调用 `enableReading()`、`enableWriting()`、`disableAll()` 最终都会进入：

```text
Channel::update
  -> EventLoop::updateChannel
  -> Poller::updateChannel
  -> epoll_ctl
```

**`tie()` 很重要。Channel 的回调通常保存所属对象的裸 `this`；事件触发时先把 weak pointer 提升为 shared pointer，可避免回调执行期间 `TcpConnection` 被析构。**

**Channel 不关闭 fd，也不决定业务行为，只负责把事件翻译成相应回调。**



## 7. Poller 与 EPollPoller

入口文件：

- 抽象接口：[`net/Poller.h`](net/Poller.h)、[`net/Poller.cc`](net/Poller.cc)
- epoll 实现：[`net/poller/EPollPoller.h`](net/poller/EPollPoller.h)、[`EPollPoller.cc`](net/poller/EPollPoller.cc)
- poll 实现：[`net/poller/PollPoller.h`](net/poller/PollPoller.h)、[`PollPoller.cc`](net/poller/PollPoller.cc)
- 实现选择：[`net/poller/DefaultPoller.cc`](net/poller/DefaultPoller.cc)

`Poller` 保存 `fd -> Channel*` 映射，但不拥有 Channel：

```cpp
using ChannelMap = std::map<int, Channel*>;
ChannelMap channels_;
```

`EPollPoller` 维护三态：

```text
kNew(-1)      从未加入 epoll
kAdded(1)     已加入 epoll
kDeleted(2)   暂时从 epoll 删除，但仍在 channels_ 中
```

读 `updateChannel()` 时重点跟踪 `EPOLL_CTL_ADD/MOD/DEL` 与 `Channel::index_` 的对应关系。读 `poll()` 时重点看 `epoll_event.data.ptr` 如何保存并取回 Channel 指针。

默认使用 epoll。设置环境变量 `MYMUDUO_USE_POLL` 可切换到 poll 实现，主要用于对照学习。

## 8. EventLoop：Reactor 核心

入口文件：[`net/EventLoop.h`](net/EventLoop.h)、[`net/EventLoop.cc`](net/EventLoop.cc)。

关键成员：

```cpp
const pid_t threadId_;
std::unique_ptr<Poller> poller_;
std::unique_ptr<TimerQueue> timerQueue_;
int wakeupFd_;
std::unique_ptr<Channel> wakeupChannel_;
std::mutex mutex_;
std::vector<Functor> pendingFunctors_;
```

主循环可以简化为：

```cpp
while (!quit_) {
    activeChannels_.clear();
    poller_->poll(timeout, &activeChannels_);
    for (Channel* channel : activeChannels_) {
        channel->handleEvent(pollReturnTime_);
    }
    doPendingFunctors();
}
```

### 8.1 线程归属

构造时记录 `threadId_`。`updateChannel()`、`removeChannel()` 等操作通过 `assertInLoopThread()` 保证只在所属线程修改 Poller。

每个线程只能存在一个 EventLoop，由 thread-local 指针 `t_loopInThisThread` 检查。

### 8.2 runInLoop 与 queueInLoop

```text
runInLoop(cb)
  当前就是 Loop 线程 -> 立即执行
  其他线程           -> queueInLoop

queueInLoop(cb)
  加锁放入 pendingFunctors_
  必要时向 eventfd 写入 1
  Loop 被唤醒后执行 doPendingFunctors
```

这是跨线程切回 I/O Loop 的标准方式。不要从工作线程直接调用 `Channel::enableWriting()` 或修改连接 Buffer。

对应测试：[`tests/event_loop_thread_test.cc`](tests/event_loop_thread_test.cc)。

## 9. TimerQueue：把定时器也变成 fd 事件

入口文件：[`net/Timer.h`](net/Timer.h)、[`net/TimerId.h`](net/TimerId.h)、[`net/TimerQueue.h`](net/TimerQueue.h)、[`net/TimerQueue.cc`](net/TimerQueue.cc)。

`TimerQueue` 创建一个 `timerfd`，再用普通 Channel 监听它。因此定时器没有另一套事件分发机制：

```text
EventLoop::runAfter/runEvery
  -> TimerQueue::addTimer
  -> addTimerInLoop
  -> timerfd_settime
  -> timerfd 可读
  -> Channel::handleEvent
  -> TimerQueue::handleRead
  -> 执行到期 TimerCallback
```

`timers_` 按到期时间排序，`activeTimers_` 用于按 Timer 指针和 sequence 查找取消项。回调仍在 EventLoop 线程执行，所以耗时任务不能直接放在定时器回调里。

## 10. Socket、地址与 Acceptor

### 10.1 Socket 和 InetAddress

- [`net/Socket.h`](net/Socket.h)：拥有一个 fd，析构时关闭；提供 bind、listen、accept、shutdownWrite 和 socket option。
- [`net/SocketsOps.h`](net/SocketsOps.h)：薄系统调用封装，创建非阻塞且 close-on-exec 的 socket。
- [`net/InetAddress.h`](net/InetAddress.h)：保存 IPv4/IPv6 sockaddr，负责字符串与网络字节序转换。

### 10.2 Acceptor

入口文件：[`net/Acceptor.h`](net/Acceptor.h)、[`net/Acceptor.cc`](net/Acceptor.cc)。

拥有关系：

```text
Acceptor
  ├── Socket acceptSocket_       拥有监听 fd
  └── Channel acceptChannel_     观察监听 fd
```

监听 fd 可读时，`handleRead()` 循环 accept，直到 `EAGAIN`。每个新 fd 交给 `NewConnectionCallback`，Acceptor 本身不创建 TcpConnection。

`idleFd_` 是经典的 EMFILE 保护：预留 `/dev/null` fd；进程 fd 耗尽时先释放它，accept 并关闭一个连接，再重新建立预留 fd。

## 11. TcpServer：接入和连接表

入口文件：[`net/TcpServer.h`](net/TcpServer.h)、[`net/TcpServer.cc`](net/TcpServer.cc)。

关键成员：

```cpp
EventLoop* loop_;                         // base loop
std::unique_ptr<Acceptor> acceptor_;
std::shared_ptr<EventLoopThreadPool> threadPool_;
ConnectionMap connections_;               // 强持有所有连接
```

启动路径：

```text
TcpServer::start
  -> EventLoopThreadPool::start
  -> baseLoop.runInLoop(Acceptor::listen)
```

新连接路径：

```text
Acceptor::handleRead
  -> TcpServer::newConnection
  -> threadPool_->getNextLoop()       选择 I/O Loop
  -> 创建 TcpConnection
  -> connections_[name] = conn       base loop 强持有
  -> 设置 connection/message/close 回调
  -> ioLoop->runInLoop(connectEstablished)
```

关闭路径：

```text
TcpConnection::handleClose           在连接 I/O Loop
  -> closeCallback
  -> TcpServer::removeConnection
  -> baseLoop::removeConnectionInLoop
  -> connections_.erase
  -> ioLoop::queueInLoop(connectDestroyed)
  -> Channel::remove
```

为什么绕回 base loop 删除连接？因为 `connections_` 归 base loop 管理，而 Channel 的最终移除必须回连接所属 I/O Loop。这个“双 Loop 交接”是理解生命周期的关键。

## 12. TcpConnection：连接生命周期与收发

入口文件：[`net/TcpConnection.h`](net/TcpConnection.h)、[`net/TcpConnection.cc`](net/TcpConnection.cc)。

### 12.1 关键结构

```cpp
enum StateE {
    kDisconnected,
    kConnecting,
    kConnected,
    kDisconnecting
};

EventLoop* loop_;
std::atomic<StateE> state_;
std::unique_ptr<Socket> socket_;       // 拥有 fd
std::unique_ptr<Channel> channel_;     // 观察 fd
Buffer inputBuffer_;
Buffer outputBuffer_;
std::shared_ptr<void> context_;        // 上层每连接状态
```

### 12.2 建立连接

`connectEstablished()` 必须在连接所属 I/O Loop 执行：

1. 状态从 `kConnecting` 变为 `kConnected`。
2. `channel_->tie(shared_from_this())` 建立事件期生命周期保护。
3. 开启读事件。
4. 调用用户的 `ConnectionCallback`。

### 12.3 读取

`handleRead()` 调用 `inputBuffer_.readFd()`：

- `n > 0`：调用 `MessageCallback`。
- `n == 0`：对端关闭，进入 `handleClose()`。
- 真实错误：记录 socket 错误；`EAGAIN/EINTR` 不作为连接错误。

业务回调必须使用 `retrieve()` 或 `retrieveAllAsString()` 消费已解析数据，否则同一数据会继续留在 Buffer 中。

### 12.4 发送

在 I/O Loop 内调用 `send()` 时直接进入 `sendInLoop()`；在其他线程调用时，会复制数据、持有连接 `shared_ptr`，再投递到所属 Loop。

`sendInLoop()` 的策略：

1. 输出 Buffer 为空时，先直接 `write`，减少一次拷贝。
2. 未写完的数据追加到 `outputBuffer_`。
3. 开启 Channel 的写事件。
4. epoll 报告可写后，`handleWrite()` 继续发送。
5. Buffer 清空后关闭写事件并触发 `WriteCompleteCallback`。

不能一直监听 POLLOUT：socket 大多数时候都可写，常驻监听会让 EventLoop 空转。

### 12.5 关闭

- `shutdown()`：进入 `kDisconnecting`；输出 Buffer 清空后执行 `shutdown(SHUT_WR)`，属于优雅半关闭。
- `forceClose()`：投递到所属 Loop 后直接触发关闭流程。
- `connectDestroyed()`：禁用事件、发出断开回调并从 Poller 移除 Channel。

`context_` 可保存协议解析器或上传会话等每连接状态，但其读写同样应遵守连接线程归属。

## 13. I/O 线程模型

入口文件：[`net/EventLoopThread.h`](net/EventLoopThread.h)、[`net/EventLoopThreadPool.h`](net/EventLoopThreadPool.h)。

```text
主线程 / base loop
  ├── Acceptor
  ├── TcpServer::connections_
  └── round-robin 分配新连接

I/O 线程 1
  └── EventLoop -> 若干 TcpConnection Channel

I/O 线程 2
  └── EventLoop -> 若干 TcpConnection Channel
```

`EventLoopThread::threadFunc()` 在线程栈上创建 EventLoop，并通过条件变量把指针交给启动线程。析构时先 `quit()`，再 `join()`，所以 `EventLoopThread` 的生命期必须覆盖返回的 EventLoop 指针。

`setThreadNum(0)` 表示所有连接都在 base loop；大于 0 时，新连接按 round-robin 分配。

## 14. TCP 客户端路径

入口文件：[`net/Connector.h`](net/Connector.h)、[`net/TcpClient.h`](net/TcpClient.h)。

`Connector` 创建非阻塞 socket，调用 `connect()`：

- 立即成功或 `EINPROGRESS`：用 Channel 监听可写事件。
- 可写后检查 `SO_ERROR`，成功则把 fd 交给 `TcpClient`。
- 失败且允许重试：通过 `EventLoop::runAfter()` 指数退避，最大 30 秒。

`TcpClient::newConnection()` 与服务端类似，会创建一个 `TcpConnection` 并设置回调。初次学习服务端时可以先跳过客户端，等理解 TcpConnection 后再读。

## 15. HTTP 层

入口文件：

- [`net/HttpContext.h`](net/HttpContext.h)：增量解析状态机。
- [`net/HttpRequest.h`](net/HttpRequest.h)：方法、路径、Header、查询参数和 body。
- [`net/HttpResponse.h`](net/HttpResponse.h)：状态行、Header 和 body 序列化。
- [`net/HttpServer.h`](net/HttpServer.h)：把 TCP MessageCallback 适配为 HTTP callback。

解析状态：

```text
kExpectRequestLine
  -> kExpectHeaders
  -> kExpectBody
  -> kGotAll
```

调用链：

```text
TcpConnection::handleRead
  -> HttpServer::onMessage
  -> HttpContext::parseRequest
  -> HttpServer::onRequest
  -> HttpCallback
  -> HttpResponse::appendToBuffer
  -> TcpConnection::send
```

当前支持请求行、Header、`Content-Length` body 和基础 keep-alive/pipelining；不支持 chunked transfer encoding。

需要特别注意：当前 `HttpContext` 会把完整 body 放入 `HttpRequest::body_`，`HttpResponse` 也保存完整 body，因此只适合小请求/响应。对象存储的大文件数据面必须先增加流式 body 回调和背压，详见 [`docs/object_storage_phase1_design.md`](docs/object_storage_phase1_design.md)。

## 16. 所有权与线程规则速查

| 对象/数据 | 所有者 | 应在哪个线程操作 |
| --- | --- | --- |
| `Poller`、`TimerQueue`、wakeup Channel | EventLoop | 所属 Loop 线程 |
| `Channel` | Acceptor、TcpConnection、TimerQueue 等上层对象 | 所属 Loop 线程更新/移除 |
| 监听 fd | Acceptor::Socket | base loop |
| 已连接 fd | TcpConnection::Socket | 连接所属 I/O Loop |
| `connections_` | TcpServer | base loop |
| `inputBuffer_`、`outputBuffer_` | TcpConnection | 连接所属 I/O Loop |
| `pendingFunctors_` | EventLoop | mutex 保护，可跨线程投递 |
| 普通任务队列 | base::ThreadPool | ThreadPool 内部加锁 |

必须记住的规则：

- `Poller` 不拥有 Channel，Channel 也不拥有 fd。
- Socket 拥有并关闭 fd。
- TcpServer 的连接表通过 `shared_ptr` 保持连接存活。
- Channel 的 `tie_` 防止事件回调执行中所属连接析构。
- 延迟或跨线程回调不要捕获生命周期不明的裸 `this`。
- 工作线程完成任务后，用 `TcpConnection::send()` 或 `EventLoop::queueInLoop()` 返回 I/O 线程。

## 17. 推荐代码学习顺序

### 第 1 轮：只建立调用链

1. [`examples/echo_server.cc`](examples/echo_server.cc)：先看用户如何创建和使用服务器。
2. [`net/Callbacks.h`](net/Callbacks.h)：认识框架向用户暴露的回调。
3. [`net/Buffer.h`](net/Buffer.h) 与 [`Buffer.cc`](net/Buffer.cc)：理解输入数据如何保存和消费。
4. [`net/Channel.h`](net/Channel.h) 与 [`Channel.cc`](net/Channel.cc)：理解 fd 如何绑定回调。
5. [`net/Poller.h`](net/Poller.h) 与 [`poller/EPollPoller.cc`](net/poller/EPollPoller.cc)：理解 epoll 如何返回 Channel。
6. [`net/EventLoop.cc`](net/EventLoop.cc)：串起 poll、事件回调和跨线程任务。

完成这一轮后，应能口述：`epoll_wait` 返回后，Echo Server 的 lambda 是怎样被调用的。

### 第 2 轮：理解服务端和连接生命期

7. [`net/Socket.cc`](net/Socket.cc)、[`SocketsOps.cc`](net/SocketsOps.cc)、[`InetAddress.cc`](net/InetAddress.cc)。
8. [`net/Acceptor.cc`](net/Acceptor.cc)：监听 fd 如何产生连接 fd。
9. [`net/TcpServer.cc`](net/TcpServer.cc)：连接如何创建、分配和放入连接表。
10. [`net/TcpConnection.cc`](net/TcpConnection.cc)：精读 `connectEstablished`、`handleRead`、`sendInLoop`、`handleWrite`、`handleClose`、`connectDestroyed`。

完成这一轮后，应能画出连接从 accept 到析构期间由哪些 `shared_ptr` 持有。

### 第 3 轮：理解多线程 Reactor

11. [`base/ThreadPool.cc`](base/ThreadPool.cc)、[`CountDownLatch.cc`](base/CountDownLatch.cc)。
12. [`net/EventLoopThread.cc`](net/EventLoopThread.cc)。
13. [`net/EventLoopThreadPool.cc`](net/EventLoopThreadPool.cc)。
14. 回到 `EventLoop::queueInLoop()` 和 `TcpConnection::send()`，理解 eventfd 唤醒及跨线程数据复制。

完成这一轮后，应能解释：磁盘线程调用 `conn->send()` 时，真正的 socket write 在哪个线程发生。

### 第 4 轮：定时器、客户端和 HTTP

15. [`net/TimerQueue.cc`](net/TimerQueue.cc)：理解 timerfd 如何融入 Reactor。
16. [`net/Connector.cc`](net/Connector.cc)、[`TcpClient.cc`](net/TcpClient.cc)：理解非阻塞连接和重试。
17. [`net/HttpContext.cc`](net/HttpContext.cc)：逐状态阅读增量解析。
18. [`net/HttpServer.cc`](net/HttpServer.cc)：理解协议适配层如何复用 TCP 回调。

### 初期可以跳过

- `base/Date.*`、`TimeZone.*`、`ProcessInfo.*`：与 Reactor 主链无关。
- `base/AsyncLogging.*`、`LogFile.*`：先知道日志可替换输出即可。
- `net/poller/PollPoller.*`：读懂 epoll 实现后再作为对照。
- `net/RouteTrie.*`：当前不属于网络核心调用链。
- `legacy/file_manager/`：旧业务，不用于理解基础层。

## 18. 建议配合代码做的练习

1. 在 `EventLoop::loop()`、`Channel::handleEventWithGuard()`、`TcpConnection::handleRead()` 设置断点，单步跟踪一次 Echo。
2. 把 Echo Server 的 `setThreadNum(2)` 改为 `0`，观察连接回调线程 ID 的变化。
3. 从一个普通 `std::thread` 调用 `conn->send()`，在 `queueInLoop()` 和 `sendInLoop()` 观察线程切换。
4. 发送超过 socket 发送缓冲的数据，在 `sendInLoop()` 和 `handleWrite()` 观察 `outputBuffer_` 与 POLLOUT。
5. 客户端主动关闭连接，跟踪 `handleRead(n == 0)` 到 `connectDestroyed()`。
6. 调用 `runAfter()`，跟踪 timerfd Channel 与普通 socket Channel 如何走同一条 EventLoop 分发路径。
7. 把一条 HTTP 请求拆成多次发送，观察 `HttpContext` 如何在多次 MessageCallback 间保留解析状态。

仓库内可直接参考：

- [`tests/tcp_server_test.cc`](tests/tcp_server_test.cc)：并发连接、跨线程发送与断开。
- [`tests/event_loop_thread_test.cc`](tests/event_loop_thread_test.cc)：跨线程投递和唤醒。
- [`tests/http_context_test.cc`](tests/http_context_test.cc)：HTTP 增量解析。

## 19. 阅读时需要警惕的当前边界

- 项目依赖 Linux 的 epoll、eventfd 和 timerfd。
- `TcpConnection::send(const void*, int)` 的长度仍是 `int`；大文件层应使用有限窗口，不应单次发送巨大数据。
- 输出 Buffer 尚无硬上限；高水位回调只是通知，不会自动阻止继续写入。
- 当前没有公开的 `startRead()/stopRead()`，尚未形成完整网络—磁盘背压。
- HTTP 请求和响应仍会聚合完整 body，不适合直接传输大文件。
- `shared_ptr<void> context_` 灵活但缺乏类型约束，转换类型由上层保证。
- `TcpServer`、Acceptor 和 EventLoop 的销毁应遵循线程归属，通常在 base loop 所在线程完成。

这些不是理解主链的前置障碍，但正是后续开发 Storage Node 流式 HTTP 时需要扩展的位置。

## 20. 最终自测问题

读完后，如果能独立回答以下问题，就已经掌握了框架主干：

1. EventLoop 为什么需要 eventfd？
2. Channel 为什么不直接拥有 fd？谁负责关闭 fd？
3. 为什么 TcpConnection 既需要 TcpServer 连接表持有，又需要 Channel::tie？
4. 为什么连接删除要先回 base loop，再回 I/O Loop？
5. 为什么不能始终监听 POLLOUT？
6. `send()` 从磁盘线程调用时，数据和连接对象如何保持有效？
7. Buffer 的 readerIndex/writerIndex 如何避免每次消费都移动内存？
8. 普通 ThreadPool 和 EventLoopThreadPool 的区别是什么？
9. timerfd 为什么可以复用 Channel/Poller 机制？
10. 当前 HTTP 实现为什么会随上传文件大小增长内存？

建议每读完一轮，就回到 [`examples/echo_server.cc`](examples/echo_server.cc)，重新从 `main()` 沿调用链走一遍。框架的类很多，但主线始终只有“注册事件、等待事件、在正确线程调用回调、管理对象生命期”四件事。
