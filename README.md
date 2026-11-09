# mymuduo network foundation

一个面向 Linux 的 C++17 Reactor 网络基础库。核心使用 epoll、eventfd 和 timerfd，提供线程、日志、事件循环、定时器、非阻塞 TCP 与可选 HTTP/1.x 协议层。

仓库包含已实现的单节点 Storage Node，以及 Coordinator V1 的控制面服务。Coordinator 使用 gRPC + Protobuf，不依赖 Reactor 网络库。原 Web 文件管理系统已移入 `legacy/file_manager/`，默认不参与构建。

## 目录

```text
base/                 线程、同步、时间和日志
net/                  Reactor、TCP、Buffer、定时器
examples/             Echo Server
storage/              ChunkStore 与 HTTP Storage Node
proto/                Coordinator V1 协议
coordinator/          内存节点表、候选放置与 gRPC Service
apps/                 Storage Node 与 Coordinator 进程入口
tests/                基础回归测试
legacy/file_manager/  原文件管理业务、页面和 SQL（仅供参考）
```

HTTP 源码暂保留在 `net/` 中，但构建为独立的 `mymuduo_http`，TCP 核心库 `mymuduo_net` 不依赖 HTTP。

## 构建与测试

要求 Linux、CMake 3.16+、支持 C++17 的编译器，以及 OpenSSL `libcrypto`
开发包（用于 Chunk 的 SHA-256 校验）。

```bash
cmake -S . -B build \
  -DMYMUDUO_BUILD_HTTP=ON \
  -DMYMUDUO_BUILD_EXAMPLES=ON \
  -DMYMUDUO_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

运行 Echo Server：

```bash
./build/bin/echo_server 2007
```

仅构建 TCP 基础库：

```bash
cmake -S . -B build-core \
  -DMYMUDUO_BUILD_HTTP=OFF \
  -DMYMUDUO_BUILD_EXAMPLES=OFF \
  -DMYMUDUO_BUILD_TESTS=OFF
cmake --build build-core -j
```

旧文件管理程序默认关闭。若只为参考而尝试构建，可设置 `MYMUDUO_BUILD_LEGACY_FILE_MANAGER=ON`，并自行准备 MySQL client、nlohmann/json 和旧业务运行环境；它不属于当前核心库的兼容性保证范围。

## Coordinator V1 开发构建

Coordinator 控制面目前完成[阶段 E：多进程验收](docs/05_Coordinator_V1_Development.md)。启用开关需要与运行库版本匹配的 `protoc`、Protobuf 开发包、`grpc_cpp_plugin` 和 `grpc++` pkg-config 包。`coordinator` 提供注册、心跳、注销、节点查询和候选放置 RPC；Storage Node 可选择启用控制代理。

```bash
cmake -S . -B build-coordinator \
  -DMYMUDUO_BUILD_COORDINATOR=ON \
  -DMYMUDUO_BUILD_HTTP=OFF \
  -DMYMUDUO_BUILD_EXAMPLES=OFF \
  -DMYMUDUO_BUILD_TESTS=OFF
cmake --build build-coordinator -j4
./build-coordinator/bin/coordinator 127.0.0.1:50051
```

默认 `MYMUDUO_BUILD_COORDINATOR=OFF`，无需 gRPC/Protobuf 即可构建原有目标。HTTP 和 Coordinator 开关可以同时开启；只有 HTTP 开启时才生成 `storage_node`。
`coordinator` 也可接收 `LISTEN_ADDRESS HEARTBEAT_MS LEASE_MS RPC_DEADLINE_MS`；租约必须大于三倍心跳间隔与 RPC deadline 之和。状态只在进程内保存，通信无认证或 TLS，部署时仅应绑定可信控制网络。

同时开启 HTTP 和 Coordinator 后，原有 `storage_node ROOT PORT` 本地模式继续可用。接入控制面时使用：

```bash
cmake -S . -B build-coordinator-http \
  -DMYMUDUO_BUILD_COORDINATOR=ON -DMYMUDUO_BUILD_HTTP=ON
cmake --build build-coordinator-http -j4
./build-coordinator-http/bin/storage_node ROOT PORT \
  127.0.0.1:50051 12345678-1234-1234-1234-123456789abc 127.0.0.1
```

Node ID 由部署方提供，重启同一逻辑节点时保持不变；每次进程启动会生成新的 instance ID。advertised host 必须能被数据面调用方访问，监听端口可以为 `0`，代理会上报实际端口。可在末尾追加 `RPC_DEADLINE_MS CONFLICT_WAIT_MS`，默认分别为 1000 和 15000；deadline 上限为 10000 ms，冲突等待上限为 600000 ms。控制代理在独立线程运行，Coordinator 不可达时现有 HTTP 接口仍可使用。候选放置不会执行 Chunk 写入。

Coordinator 重启会清空节点表，运行中的代理通过重新注册恢复发现能力。若同一 Node ID 的旧、新实例在重启后同时存活，内存表无法还原重启前的接管顺序；先注册的实例可能取得该 ID。部署时应避免两代实例同时运行。多进程验收可用 `ctest --test-dir build-coordinator-http -R coordinator_multi_process_test --output-on-failure` 运行。

单副本对象功能已完成[开发文档](docs/06_Single_Replica_Object_Development.md)的阶段 A 至 E 验收（可选的 Abort 尚未实现）：启用 `MYMUDUO_BUILD_COORDINATOR=ON` 和 `MYMUDUO_BUILD_OBJECTS=ON` 后，需有 SQLite3 与 libcurl 开发包。使用 `coordinator LISTEN_ADDRESS [HEARTBEAT_MS LEASE_MS RPC_DEADLINE_MS] --db SQLITE_PATH` 启动对象 Catalog；同一个数据库路径只允许一个 Coordinator 实例持有。当前提供 `BeginUpload`、`GetUpload`、`ConfirmChunk`、`CommitUpload`、`GetObject` 元数据 RPC。`ConfirmChunk` 会向计划目标发送有超时的 HTTP HEAD，八项描述符全部匹配后才记录确认位置。节点 Registry 仍只在内存中。

`object_client` 支持空文件和最大 64 GiB 的固定 64 MiB 多 Chunk 对象：流式预扫描后按块 PUT、确认、提交；下载按确认位置顺序 GET，逐块及全文件校验摘要，并用无覆盖原子重命名发布。使用方式：

```bash
./build-object/bin/object_client upload 127.0.0.1:50051 input.bin [STATE_PATH]
./build-object/bin/object_client download 127.0.0.1:50051 OBJECT_ID output.bin
```

上传时默认在源文件旁创建 `<FILE>.object-upload` 侧车和持久保留的 `<FILE>.object-upload.lock` 锁文件，也可通过 `STATE_PATH` 指定侧车位置；侧车在首次 Begin 前同步落盘，上传中断后须使用同一源文件和侧车路径重跑命令。两个 Client 不能同时推进同一侧车；源文件内容变化会被拒绝。成功提交后删除侧车，锁文件保留。CLI 对网络/提交结果不确定的情况会保留侧车并报告失败，不将其当作成功。V1 尚不支持节点永久失效后的自动换目标、Abort 或孤儿 Chunk 回收。

架构审查、修复记录和后续使用边界见 [PROJECT_ARCHITECTURE.md](PROJECT_ARCHITECTURE.md)。

分布式对象存储的第一阶段设计见 [docs/01_object_storage_phase1_design.md](docs/01_object_storage_phase1_design.md)。

单节点 Storage Node 的启动方式、HTTP 请求头及背压语义见 [docs/03_storage_node_http.md](docs/03_storage_node_http.md)。

文档阅读顺序见 [docs/README.md](docs/README.md)；单副本对象上传下载的设计与验收记录见 [设计与任务](docs/06_Single_Replica_Object_Development.md)。

现有 Reactor/TCP/HTTP 源码的分层导览和推荐阅读顺序见 [NETWORK_FRAMEWORK_GUIDE.md](NETWORK_FRAMEWORK_GUIDE.md)。
