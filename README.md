# mymuduo network foundation

一个面向 Linux 的 C++17 Reactor 网络基础库。核心使用 epoll、eventfd 和 timerfd，提供线程、日志、事件循环、定时器、非阻塞 TCP 与可选 HTTP/1.x 协议层。

当前仓库只承担后续 Storage Node 的网络基础能力。Coordinator 将独立使用 gRPC + Protobuf，不依赖本库。原 Web 文件管理系统已移入 `legacy/file_manager/`，默认不参与构建。

## 目录

```text
base/                 线程、同步、时间和日志
net/                  Reactor、TCP、Buffer、定时器
examples/             Echo Server
tests/                基础回归测试
legacy/file_manager/  原文件管理业务、页面和 SQL（仅供参考）
```

HTTP 源码暂保留在 `net/` 中，但构建为独立的 `mymuduo_http`，TCP 核心库 `mymuduo_net` 不依赖 HTTP。

## 构建与测试

要求 Linux、CMake 3.16+ 和支持 C++17 的编译器。

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

架构审查、修复记录和后续使用边界见 [PROJECT_ARCHITECTURE.md](PROJECT_ARCHITECTURE.md)。

分布式对象存储的第一阶段设计见 [docs/object_storage_phase1_design.md](docs/object_storage_phase1_design.md)。

现有 Reactor/TCP/HTTP 源码的分层导览和推荐阅读顺序见 [NETWORK_FRAMEWORK_GUIDE.md](NETWORK_FRAMEWORK_GUIDE.md)。
