# 网络基础层架构审查与整理记录

## 1. 当前定位

本仓库已从 Web 文件管理应用整理为可独立构建的网络基础层，服务于后续 Storage Node。分布式存储采用控制面与数据面分离：

- Coordinator：负责元数据、节点管理和数据放置，后续直接采用 gRPC + Protobuf，不依赖本网络库。
- Storage Node：负责大文件分块传输、磁盘 I/O 和副本数据读写，可复用 `base`、TCP Reactor，并按需使用 HTTP。
- 本阶段不实现 RPC、分块存储、复制、故障恢复或新的分布式存储模块。

## 2. 审查结论

### `base`：保留

通用组件包括 `Thread`、`ThreadPool`、锁与条件变量、`CountDownLatch`、时间戳、日志和文件工具。它们不依赖文件管理业务，可作为网络层及 Storage Node 后台任务的基础设施。

`ThreadPool` 适合阻塞磁盘任务或计算任务，但任务完成后若要操作连接，应通过 `TcpConnection::send` 或 `EventLoop::queueInLoop` 回到连接所属 I/O Loop。不要在多个线程直接修改同一个 `Buffer`。

### `net` TCP Reactor：保留

主要调用链为：

```text
EPollPoller -> EventLoop -> Channel -> TcpConnection -> Buffer
                       \-> Acceptor -> TcpServer -> EventLoopThreadPool
```

- `EventLoop`：one-loop-per-thread，使用 eventfd 跨线程唤醒。
- `Channel` / `EPollPoller`：管理 fd 关注事件和就绪分发。
- `Acceptor` / `Socket`：管理非阻塞监听和连接 fd。
- `TcpServer` / `TcpConnection`：管理连接分配、生命周期、输入输出缓冲和回调。
- `EventLoopThreadPool`：按 round-robin 把新连接分配到 I/O Loop。
- `TimerQueue`：基于 timerfd 的定时任务。
- `TcpClient` / `Connector`：保留为通用 TCP 客户端能力。

这些组件与原业务没有直接耦合，适合作为 Storage Node 的长连接和数据传输基础。

### HTTP：可选保留

`HttpServer`、`HttpContext`、`HttpRequest`、`HttpResponse` 属于协议适配层，不再编入 TCP 核心库。`MYMUDUO_BUILD_HTTP=OFF` 时可只构建 `mymuduo_base` 与 `mymuduo_net`。

HTTP 解析器支持请求行、请求头、`Content-Length` 请求体和 keep-alive/pipelining 的基础处理；当前明确不支持 `Transfer-Encoding: chunked`，收到后返回解析错误。原先写在通用 `HttpServer` 中的 `/upload` 大文件业务特判已移除。

### 原文件管理业务：移入 legacy

以下内容高度耦合 MySQL、本地上传目录、用户会话、分享规则和前端页面，已整体移入 `legacy/file_manager/`：

- 注册、登录、会话和用户查询；
- 文件上传、下载、删除与分享；
- MySQL schema 和数据访问；
- HTML、JavaScript、图片和 favicon；
- 旧路由实验程序。

该目录保留作实现参考，默认不编译，也不再向核心库引入 MySQL、nlohmann/json 或前端资源依赖。

## 3. 已修复的明确问题

1. `TcpConnection` 跨线程发送原先捕获指向调用方内存的 `StringPiece` 和裸 `this`，任务延迟执行时可能访问悬空数据或对象。现在复制待发送数据并由 `shared_ptr` 保持连接存活。
2. `shutdown()` 的跨线程任务改为持有连接对象，避免裸指针生命周期风险。
3. 发送缓冲未清空时不再立即调用或排队调用 `handleWrite()`；写入由 epoll 的可写事件继续驱动，避免忙轮询占用 I/O Loop。
4. 连接回调均检查是否已设置；销毁处同时处理 `connected` 与 `disconnecting` 状态，确保 Channel 在移除前禁用事件。
5. `EventLoop` 的轮询超时由 1 ms 恢复为事件驱动的长等待，跨线程任务仍由 eventfd 立即唤醒；eventfd 已满时按预期忽略 `EAGAIN`。
6. `EventLoopThread` 的退出读取受锁保护，并保证已启动线程最终 join。
7. `ThreadPool` 消除了停止标志的无锁读取；`stop()` 会唤醒工作线程并排空已提交任务后退出。
8. `Buffer::readFd` 和指针差值使用显式安全类型转换，并覆盖缓冲整理及 fd 读取测试。
9. socket 的 `bind/connect` 地址长度按 IPv4/IPv6 选择，不再固定传递 `sockaddr_in6` 大小；accept 会排空当前积压队列，并正确处理 `EAGAIN`/`EMFILE`。
10. Connector 的跨线程启动、停止和 Channel 重置任务持有自身；TcpClient 地址查询失败时不再泄漏 fd。
11. HTTP 解析移除了非 NUL 结尾缓冲区的字符串日志读取；严格校验 `Content-Length`，拒绝冲突长度以及 chunked 与 Content-Length 混用。
12. HTTP 头查询改为大小写不敏感，响应默认生成 `Content-Length`，并支持同一输入缓冲中的顺序请求。

## 4. 构建边界

目标依赖关系如下：

```text
mymuduo_base
    ^
    |
mymuduo_net          (TCP Reactor，不依赖 HTTP/MySQL)
    ^
    |
mymuduo_http         (可选)

legacy http_upload   (可选且默认关闭，依赖 HTTP/MySQL/json)
```

CMake 导出别名为 `mymuduo::base`、`mymuduo::net` 和可选的 `mymuduo::http`。默认构建核心、HTTP、示例和测试，旧业务默认关闭。

## 5. 回归测试范围

- `buffer_test`：追加、消费、整理/扩容和 fd 读取。
- `thread_pool_test`：并发任务执行以及停止时排空队列。
- `event_loop_thread_test`：从其他线程投递任务、eventfd 唤醒和 Loop 退出。
- `tcp_server_test`：动态端口监听、24 个并发客户端、连接建立、跨线程 Echo 发送、完整收发和主动断开。
- `http_context_test`：增量请求体、大小写不敏感头、非法 Content-Length 和不支持的 chunked 请求。

测试不依赖第三方测试框架。由于实现使用 Linux 专有的 epoll/eventfd/timerfd，完整构建和运行必须在 Linux 完成；非 Linux 配置会立即给出明确错误。

## 6. Storage Node 可直接复用的接口

- `TcpServer(EventLoop*, InetAddress, name)`：监听数据面端口。
- `TcpServer::setThreadNum()`：配置 I/O Loop 数量。
- `TcpServer::setConnectionCallback()`：初始化或清理每连接上下文。
- `TcpServer::setMessageCallback()`：消费 `Buffer` 中的自定义分块协议数据。
- `TcpConnection::send()`：可从磁盘工作线程安全地投递响应；实现会复制跨线程数据。
- `TcpConnection::shutdown()` / `forceClose()`：控制连接关闭。
- `TcpConnection::setContext()`：挂载每连接的协议解析或传输状态。
- `Buffer`：增量协议解析与非阻塞读缓冲。
- `EventLoop::queueInLoop()` / `runInLoop()`：把状态变更切回所属 I/O 线程。
- `EventLoop::runAfter()` / `runEvery()`：实现连接超时、心跳或后台检查。
- `ThreadPool`：承载不能阻塞 I/O Loop 的磁盘任务。

后续实现数据面协议时，建议保持“每个连接状态只在所属 I/O Loop 修改；磁盘线程只处理独立任务，完成后投递结果”的边界。大块数据应使用有界任务队列和 `HighWaterMarkCallback` 做背压，但本阶段不提前设计具体分块、校验、副本或恢复协议。
