# 单节点 Storage Node HTTP 数据面

## 启动与范围

```bash
cmake -S . -B build -DMYMUDUO_BUILD_HTTP=ON -DMYMUDUO_BUILD_TESTS=ON
cmake --build build -j4
./build/bin/storage_node /path/to/storage-root 8080
```

服务监听指定端口；`0` 表示由内核分配端口。进程收到 `SIGINT` 或 `SIGTERM` 后停止 EventLoop，等待已提交的磁盘任务和 Writer/Reader 清理完成，再销毁 ChunkStore。ChunkStore 在启动恢复前独占存储根目录；第二个使用同一 root 的进程会启动失败。服务没有认证或 TLS，应只部署在可信网络中。

当前每个 TCP 连接只处理一个 HTTP/1.1 请求，然后关闭连接。上传必须使用 `Content-Length`；不支持 `Transfer-Encoding: chunked`、HTTP pipelining 或长连接复用。除下面的 PUT 外，现有普通 HTTP 解析行为保持不变。

## 请求协议

路径均为 `/v1/chunks/{chunk_id}`，其中 ID 是规范化的小写 UUID。

### PUT：上传一个连续片段

必需请求头：

| 请求头 | 含义 |
| --- | --- |
| `X-Upload-Id`、`X-Object-Id` | 上传与对象 UUID |
| `X-Object-Version`、`X-Chunk-Index`、`X-Logical-Offset` | Chunk 描述符字段 |
| `X-Chunk-Size`、`X-Checksum-Sha256`、`X-Generation` | 完整 Chunk 的大小、SHA-256 小写十六进制值和代数 |
| `X-Offset` | 本次片段相对 Chunk 起点的连续写入偏移 |
| `Content-Length` | 本次片段的字节数，可以小于完整 Chunk 大小 |

`X-Expires-At-Micros` 可选，`0` 表示不指定到期时间。第一次 PUT 建立 Writer；已有 staging 目录时恢复同一上传，并校验描述符和连续偏移。片段可以大于内部窗口，服务按 **64 KiB** 分次 Append 和 Checkpoint。正常断线不会 Abort，已完成的检查点保留在磁盘上。断线时恰好在途的窗口可能仍完成，应以新的 HEAD 查询可续传偏移。

- `202 Accepted`：片段写入成功但完整 Chunk 尚未提交；`X-Durable-Offset` 表示已持久化前缀。
- `201 Created`：完整 Chunk 的 SHA-256 校验及 Commit 成功；返回 `X-Durable-Offset`。
- `200 OK`：描述符一致的 Chunk 已提交；服务按窗口读完重复请求体后响应。
- `409 Conflict`：偏移或描述符冲突、同一 Chunk 有其他活动 Writer；偏移冲突时返回 `X-Durable-Offset`。重复提交未完成片段应先按该偏移或 HEAD 的结果决定下一步。
- `422 Unprocessable Entity`：最终 checksum 不匹配，且 Abort 已成功。
- `503 Service Unavailable` 加 `X-Commit-State: unknown`：Commit 或随后的 Abort 进入结果不确定状态。客户端不能据此直接判定提交失败或重写同一 Chunk；应查询 HEAD，必要时重启并执行恢复后再确认持久化状态。

### HEAD：状态和续传位置

请求头 `X-Generation` 必需。已提交 Chunk 返回 `200`，并带 `X-Chunk-Size`、`X-Checksum-Sha256`、`X-Generation`。若希望查询未提交的 staging 上传，另带 `X-Upload-Id`；存在可恢复尝试时返回 `308 Resume Incomplete` 与 `X-Durable-Offset`。活动 Writer 占用该 Chunk 时可能返回 `409`，客户端稍后重试。不存在返回 `404`。

### GET：流式下载与 Range

请求头 `X-Generation` 必需。可选标准单段 Range，例如 `Range: bytes=100-199`、`bytes=100-` 或 `bytes=-100`。完整读取返回 `200`，合法 Range 返回 `206` 和 `Content-Range`；无效范围返回 `416`。响应包含完整 Chunk 的 SHA-256 元数据，客户端可据此校验下载数据。普通 GET 只进行 ChunkStore 的轻量结构校验，**不会为每次下载重新扫描全量 SHA-256**。

### DELETE：删除已提交 Chunk

请求头 `X-Generation` 必需。删除成功或已提交 Chunk 不存在时返回 `204`；代数不匹配或有活动 Writer 时返回 `409`。DELETE 不取消 staging 上传。

## 数据流、背压与关闭

网络线程只解析头、管理连接和传递固定大小窗口。`HttpContext` 在请求头结束处暂停解析；PUT body 不进入 `HttpRequest::body()`。每连接同一时刻最多有一个上传或下载磁盘任务。上传窗口交给有界磁盘线程池后暂停 TCP 读取，完成 Checkpoint 后才恢复；下载在上一窗口写完后才从 ChunkReader 读取下一块。网络输入、用户态输出与单个任务负载均按连接保持有界，**不会把整个 Chunk 放入内存**。

默认最多 256 个连接、4 个磁盘工作线程、64 个待处理磁盘任务；队列满时非阻塞地拒绝新任务。Writer/Reader 的最终关闭由单独的有界清理线程执行，服务关闭顺序为停止新工作、排空磁盘任务及其 EventLoop 完成回调、释放连接上下文、等待清理线程，最后析构 ChunkStore。断线后不再发送响应，但已提交的磁盘任务会完成并保留可恢复检查点。空闲连接默认 60 秒关闭，响应完成后最多再等待客户端关闭 5 秒。测试可用 `STORAGE_NODE_DISK_DELAY_MS` 和 `STORAGE_NODE_IDLE_TIMEOUT_SEC` 环境变量模拟慢磁盘和短空闲超时。

固定窗口、连接数和任务队列使慢客户端或慢磁盘无法让**用户态缓冲无限增长**；内核 socket 缓冲和所有连接的总内存仍随配置的连接上限增长。集成测试以 8 MiB Chunk、慢读客户端和 20 ms/窗口磁盘延迟验证数据内容及进程高水位内存，但这不是所有负载下的形式化内存证明。

## 已知限制

- 只支持已知长度的单请求连接；没有 HTTP chunked、keep-alive、TLS、认证或请求签名。
- 对已提交 Chunk 的重复 PUT 按描述符认定幂等，不重新校验重复请求体；未完成片段的重复发送返回当前持久化偏移，客户端须据此续传。
- 当前没有单独的后台全量 checksum 巡检。GET 返回已存储的 checksum，客户端负责验证数据。
- ChunkStore 的 Commit/Abort 中间失败和 `trash` 清理仍未在存储层彻底解决。`X-Commit-State: unknown` 明确暴露不确定结果，不宣称失败重试一定安全。
- 线程池中的在途文件操作不作强行中断；断线及退出采用完成任务、丢弃响应并释放资源的方式。

## P0 修复报告：root 排他与目录持久化

### 根因与修改范围

- 之前只有进程内的 Chunk 锁和活动 Writer 登记。两个 Storage Node 进程可对同一
  root 同时启动，各自的锁无法约束对方。现在 ChunkStore 在执行恢复前对
  `<root>/.chunk-store.lock` 取得非阻塞排他锁，并持有到 Store 析构。
  锁文件保留在磁盘上，不应在服务运行时删除或替换；锁由操作系统在进程退出时释放。
- 之前新建 staging 上传目录后只同步其内部目录，未同步 `staging/` 中的新目录项；
  正常提交和启动恢复跨目录发布时只同步最终目录的父目录。现在新建存储根目录及
  staging 目录时同步相应父目录；正常提交和恢复发布均在目录移动后同步源、目标
  父目录。初始化、续传和读取已提交元数据时重新确认可见目录的持久化，避免把
  上一次同步失败留下的目录直接当成已持久化。Store 的公开接口、磁盘数据格式
  和 HTTP 协议均未改变。

### 错误语义与验证

- root 已被占用时，初始化返回冲突，服务在处理请求前退出。目录同步失败不会被
  报告为成功：新建上传返回 I/O 错误；提交目录已移动但同步失败时，结果仍属
  不确定，HTTP 层继续返回 `503` 和 `X-Commit-State: unknown`，调用方须查询或
  重启恢复后确认。失败操作可能留下待恢复目录，不应直接盲目重试提交。
- 新增 ChunkStore 回归测试：同进程和跨进程 root 竞争及锁释放；新建 root、
  staging 父目录同步失败后的重试；提交时源父目录同步失败的错误传播及
  重启后目录同步；目标父目录同步失败后即时查询仍返回错误，重新同步成功后才
  返回已提交；中断提交恢复时源父目录同步。Storage Node socket 集成测试另验证
  第二个进程在同一 root 启动失败。
- 普通 Debug 和 ASan/UBSan 构建均成功；两套完整 CTest 均为 **8/8 通过**。
  测试通过定向注入 `fsync` 错误验证调用和返回语义，不等同于物理断电试验。
  `flock` 是协作式本地文件锁；非协作进程或人为替换锁文件仍可能绕过保护。
  本修复未处理上文列出的其他已知限制。
