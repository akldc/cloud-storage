# 开发状态

## 项目目标与当前阶段

本仓库是一个 C++ 对象存储项目，重点是存储引擎和基础架构能力，包括 Chunk 持久化、崩溃恢复、有界流式 I/O，以及后续的分布式扩展。目前已实现单节点本地 `ChunkStore`，并新增可独立运行的 Storage Node HTTP 数据面，支持单段流式 PUT、HEAD 续传查询、GET/Range 和 DELETE。尚未实现 Coordinator 或多副本功能。

## 职责、所有权与生命周期

- `ChunkStore` 管理存储根目录配置、启动恢复、Chunk 锁和活动 Writer 登记。Store 对象由调用方拥有。
- `ChunkWriter` 以 `std::unique_ptr` 返回，由调用方拥有。它持有数据文件描述符，并通过裸指针引用创建它的 Store。活动登记用于阻止同一 Chunk 同时存在多个活动 Writer。Store 中另有独立计数，记录**所有仍存活的 Writer 对象**，包括已经 Commit 或 Abort、但尚未析构的对象。
- `ChunkReader` 以 `std::unique_ptr` 返回，持有已打开的文件描述符，以及描述符和读取范围的副本；它不反向引用 Store。当前头文件要求 Reader 不超过 Store 的生命周期，但实现中没有 Store 访问，也没有检测违反该约束的机制。

关闭时，调用方必须先停止发起 Store 和 Writer 操作，等待在途操作结束，销毁所有 Writer，最后析构 Store。Store 析构不得与任何 Store API 调用或 Writer 操作、析构并发。如果析构 Store 时仍有 Writer 对象存活，Store 会在所有构建模式下调用 `std::terminate()`。这只能检测违规顺序；存活计数**不使并发析构安全**。Writer 析构不得与其方法调用并发；Reader 析构也不得与 `ReadAt` 并发。

本阶段保留调用方直接管理 Store 的方式，没有改用 `shared_ptr<ChunkStore>` 和 Writer 中的 `weak_ptr`。Storage Node 可以控制明确的关闭顺序；共享所有权会改变现有调用方式，还会引入 Store 已销毁后上传操作返回错误的路径，却不能解决 Writer 自身并发析构等状态竞态。存活计数与活动 Writer 登记用途不同，仅在 Writer 对象析构时减少。

## Delete 与活动写入

`Delete` 获取 Chunk 状态转换锁，并在持锁期间检查活动 Writer 登记。如果该 Chunk 有活动 Writer，返回 `kConflict`，不自动取消上传，也不执行删除。没有活动 Writer 时，沿用原有的 generation 校验和移动目录删除逻辑。`BeginWrite`、`ResumeWrite`、`Commit`、`Abort` 使用同一 Chunk 锁处理状态转换；涉及活动登记时，锁顺序为先获取 Chunk 锁，再获取登记互斥锁。Writer 的方法调用另由 Writer 自身的互斥锁串行化。

## ChunkStore 生命周期阶段的修改与验证

本阶段修改了 `storage/chunk_store/ChunkStore.h`、`ChunkStore.cc`、`ChunkWriter.cc` 和 `tests/storage/chunk_store_test.cc`。新增测试覆盖正常的 Writer/Store 析构顺序、在独立子进程中验证未完成及已 Commit/Abort 但仍存活的 Writer 触发 fail-fast、Writer 创建失败路径，以及 Delete 冲突后 Abort 或 Commit 与正常删除的结果。此前工作区中与本阶段无关的 HTTP 修改得到保留。

在 Ubuntu 22.04.5、GCC 11.4、CMake 3.22.1 和 OpenSSL 3.0.2 环境下，启用 `-Wall -Wextra -Wpedantic` 的 Debug 构建无编译警告。ChunkStore 单项测试通过；完整 CTest 为 **7/7 通过、0 跳过**；ASan/UBSan 完整 CTest 也是 **7/7 通过、0 跳过**。TCP 和 Sanitizer 测试在允许本地 socket 与 LeakSanitizer 正常工作的执行环境中运行；受限沙箱会阻止这些能力。

验证命令：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS='-Wall -Wextra -Wpedantic' \
  -DMYMUDUO_BUILD_HTTP=ON -DMYMUDUO_BUILD_EXAMPLES=ON \
  -DMYMUDUO_BUILD_TESTS=ON
cmake --build build -j4
ctest --test-dir build -R '^chunk_store_test$' --output-on-failure
ctest --test-dir build --output-on-failure

cmake -S . -B build-sanitize -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS='-Wall -Wextra -Wpedantic -fsanitize=address,undefined -fno-omit-frame-pointer' \
  -DCMAKE_EXE_LINKER_FLAGS='-fsanitize=address,undefined' \
  -DMYMUDUO_BUILD_HTTP=ON -DMYMUDUO_BUILD_EXAMPLES=ON \
  -DMYMUDUO_BUILD_TESTS=ON
cmake --build build-sanitize -j4
ctest --test-dir build-sanitize --output-on-failure
git diff --check
```

## 遗留问题与下一步

- Commit 或 Abort 可能在关闭文件描述符或移动目录后失败，使进程内 Writer 的重试行为不明确。本阶段没有修改磁盘协议或这些中间状态。
- 多个 Store 实例或进程使用同一 root 时，没有根目录级排他锁。当前必须由部署方保证独占。
- 已 Abort 或过期的上传目录会移至 `trash`，但尚无垃圾清理机制。
- 现有测试未证明所有并发 I/O、崩溃点和文件系统错误交错下的正确性。Store 的关闭顺序仍需调用方保证。
- 无 Range 的 `OpenRead` 先单独调用 `Stat`，再打开文件描述符；并发 Delete 可能使后一步返回 `NotFound`。

## Storage Node HTTP 阶段

单节点 Storage Node HTTP 数据面已实现并通过测试。 可用 ./build/bin/storage_node ROOT PORT 启动；请求协议见 03_storage_node_http.md (docs/03_storage_node_http.md)。

新增服务支持分段 PUT、HEAD 查询持久化偏移、GET 与单段 Range、DELETE，以及已提交 Chunk 的重复上传。PUT 按 64 KiB 窗口交给磁盘线程 Append 和 Checkpoint；断线保留已持久化前缀。Commit 结果不确定时返回 503 和 X-Commit-State: unknown，要求调用方查询状态或经重启恢复确认。

接口调整集中在 HttpContext (net/HttpContext.h)、TcpConnection (net/TcpConnection.h)、ThreadPool (base/ThreadPool.h) 和 EventLoop (net/EventLoop.h)：分别增加头部完成后暂停解析、暂停/恢复读取、向有界队列非阻塞提交，以及退出时排空完成回调。服务实现在 StorageNode.cc (storage/node/StorageNode.cc)，原有普通 HTTP 解析入口保持可用。

内存与关闭： PUT body 不进入 HttpRequest::body()；每连接最多一个在途磁盘任务。上传处理期间暂停读取，下载在前一窗口写完后才读下一块，因此不会把整个 Chunk 放入用户态内存，慢磁盘或慢客户端也不会使单连接缓冲无限增长。默认上限为 256 个连接、64 个待处理磁盘任务，并设有空闲超时。断线不自动 Abort；退出时排空磁盘任务及回调，在清理线程释放 Writer/Reader 后才析构 Store。

验证： cmake --build build -j4、ctest --test-dir build --output-on-failure 和对应的 build-sanitize ASan/UBSan 构建与测试均通过：8/8 通过、0 跳过，无编译警告。新增 集成测试 (tests/integration/storage_node_test.py)覆盖 8 MiB 上传下载、Range、续传与重启、重复请求、并发连接、慢读客户端、人工磁盘延迟、活动写入时的删除冲突及上传中的服务关闭；进程高水位内存检查也通过。HTTP 关闭的核心构建与 git diff --check 均通过。

已知限制：当前每连接只处理一个已知长度请求；尚无认证、TLS、chunked 传输、同一 root 的进程间排他锁或 trash 清理。GET 返回存储的 SHA-256 供客户端校验，不在每次读取时重新计算全量校验和。内存测试验证了上述场景，尚不构成所有负载下的形式化上界证明。

本阶段新增 `storage/node/StorageNode.*`、`apps/storage_node.cc` 与真实 socket 集成测试，并扩展 HTTP 头部暂停解析、TCP 暂停/恢复读取、线程池非阻塞提交和 EventLoop 退出后回调排空。PUT/GET 使用固定 64 KiB 数据窗口、单连接一个在途磁盘任务、有界队列与连接上限。断线保留检查点；服务退出时先排空任务与资源，再析构 Store。协议和限制见 `docs/03_storage_node_http.md`。

在 Ubuntu 22.04.5 上，启用 `-Wall -Wextra -Wpedantic` 的完整构建无警告；新增集成测试与原有测试组成的 CTest **8/8 通过、0 跳过**，ASan/UBSan 构建与完整 CTest 也 **8/8 通过、0 跳过**。`MYMUDUO_BUILD_HTTP=OFF` 的核心配置及构建通过。集成测试覆盖 8 MiB 上传下载、Range、续传与重启、重复上传、并发连接、慢读客户端、人工磁盘延迟、活动写入时删除冲突、服务在上传期间关闭、空闲连接超时，以及进程高水位内存检查。具体命令沿用上面的构建命令，完整测试使用 `ctest --test-dir build --output-on-failure` 和 `ctest --test-dir build-sanitize --output-on-failure`。

当前仍需处理身份认证、root 进程间排他、trash 清理、Commit/Abort 中间失败的底层状态，以及更广泛的故障注入和长期负载验证。HTTP 层对不确定提交结果返回明确的未知状态，不把它当作已失败或已成功。


## ChunkStore阶段的补充提醒
通过当前单元测试，不代表 ChunkStore 已经具备生产级崩溃一致性。

尤其是 Codex 提到的 Commit/Abort 中间失败后状态不明确，需要在 HTTP 层特别小心，不能简单地收到任何异常就自动重新执行 Commit。

我建议本阶段先明确以下规则：
1. HTTP 请求断开时，关闭当前 Writer，保留已经成功 Checkpoint 的可恢复数据。
2. Commit 失败时，如果结果不确定，先通过 Stat、恢复或重新打开的方式核实状态，再决定返回什么错误；不要直接将其视为未提交。
3. HTTP 服务必须先停止接受新连接和任务，再等待在途磁盘操作和 Writer 释放，最后销毁 ChunkStore。
4. 单个 Storage Node 进程独占一个存储根目录。当前不支持多个进程共享。
另一个需要关注的点是：ChunkStore 析构时发现存活 Writer 就 std::terminate()，属于故意采用的 fail-fast 设计，可以帮助暴露错误关闭顺序，但不能代替正确的服务退出管理。

这些问题暂时不需要全部重新设计，但 HTTP 集成时必须尊重现有约束。
