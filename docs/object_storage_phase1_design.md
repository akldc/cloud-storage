# 轻量级分布式对象存储：第一阶段设计

## 1. 目标与边界

系统采用控制面与数据面分离的 Coordinator + Storage Node 架构：

- Coordinator 管理对象元数据、上传会话、节点状态和 Chunk 放置计划，控制面使用 gRPC + Protobuf。
- Storage Node 管理 Chunk 的本地持久化、校验和数据读写，数据面优先使用 HTTP，并复用现有 Reactor 网络库。
- Client 向 Coordinator 获取上传或下载计划，文件数据直接与 Storage Node 交互，不经过 Coordinator。

第一阶段只确定稳定的数据模型、单节点 `ChunkStore` 语义和模块边界。暂不实现副本复制、故障恢复、重平衡、自研 RPC、复杂数据库、零拷贝或磁盘调度优化。

设计原则：

1. Chunk 一旦提交即不可变；更新对象通过创建新版本和新 Chunk 完成。
2. 网络协议只是适配层，`ChunkStore` 不依赖 HTTP、TCP 连接或 `Buffer`。
3. 成功提交必须有明确的持久化边界；进程崩溃后不会暴露半个 Chunk。
4. 所有写入、提交和删除接口均应具备幂等语义。
5. 数据模型提前保留版本、generation 和副本位置字段，但当前不实现多副本行为。

## 2. 建议目录与依赖

```text
base/                         已有线程、同步、时间和日志库
net/                          已有 TCP Reactor 与可选 HTTP 库

storage/
├── common/                   ID、Checksum、错误码和公共模型
├── chunk_store/              单节点 Chunk 持久化，不依赖网络层
└── node/                     Storage Node 生命周期和 HTTP 数据面适配

coordinator/                  后续控制面服务、元数据和放置策略
client/                       后续上传、续传和下载客户端
proto/                        后续 gRPC/Protobuf 控制面定义
apps/                         coordinator、storage_node、client 可执行入口
tests/
├── storage/                  ChunkStore 单元与崩溃恢复测试
└── integration/              后续端到端测试

legacy/file_manager/          原文件管理应用，仅供参考
```

建议目标依赖：

```text
object_store_common
    ├── chunk_store                 -> mymuduo::base
    ├── storage_http                -> chunk_store + mymuduo::http
    ├── coordinator_service         -> common + gRPC/Protobuf（后续）
    └── object_store_client         -> common + gRPC stub + HTTP client（后续）
```

Coordinator 不链接 `mymuduo::net`。`chunk_store` 也不链接网络库，使其可以用普通文件测试直接验证。只有 Storage Node 的 HTTP 适配层依赖现有 Reactor。

### 2.1 可直接复用的现有组件

- `base::Logging`：节点、恢复和磁盘错误日志。
- `base::ThreadPool`：暂时承载阻塞磁盘读写、校验任务；不能在 I/O Loop 执行磁盘操作。
- `EventLoop` / `EventLoopThreadPool` / `TcpServer`：Storage Node 的连接接入和 I/O 线程。
- `TcpConnection`：连接生命周期、跨线程发送、写完成及高水位回调。
- `Buffer`：HTTP 头和有限大小的流式数据窗口，不作为整个 Chunk 的容器。
- `HttpContext` / `HttpServer`：可保留控制请求处理方式，但上传与下载需要增加流式接口，不能直接复用当前“完整 body”回调。

## 3. 核心数据模型

所有持久化整数采用明确宽度；大小和偏移使用 `uint64_t`。外部 ID 使用 128 位随机 ID（规范化 UUID 字符串），内部可保存为 16 字节。ID 不复用。

### 3.1 Object

`Object` 是用户可见的逻辑对象，元数据由 Coordinator 持有。

```text
ObjectMetadata
  object_id           ObjectId       稳定对象 ID
  namespace           string         租户或 bucket，第一阶段可固定为 default
  object_key          string         用户可见名称；需规范化并限制长度
  version             uint64         每次覆盖递增，禁止原地修改旧版本
  total_size          uint64
  chunk_size          uint32         本版本采用的目标分块大小
  object_checksum     Checksum?      可选的全对象校验和
  state               CREATING | AVAILABLE | DELETING | DELETED
  chunks              vector<ChunkRef>
  created_at          int64          UTC Unix 微秒
  committed_at        int64?
```

`ChunkRef` 按 `index` 排序，包含 `chunk_id`、逻辑 `offset`、实际 `size`、校验和以及未来的放置信息。最后一个 Chunk 可以小于 `chunk_size`。

初始建议默认 Chunk 大小为 64 MiB，并允许部署配置在 8–256 MiB 范围内调整；该值属于对象版本元数据，不能依赖节点本地默认值推导。

### 3.2 Chunk

Chunk 是不可变的持久化和校验单位。Chunk ID 由 Coordinator 在上传计划中预分配，不能仅使用文件名、对象 key 或数组下标生成。

```text
ChunkDescriptor
  chunk_id            ChunkId
  object_id           ObjectId
  object_version      uint64
  index               uint32
  logical_offset      uint64
  size                 uint64
  checksum             Checksum { algorithm, bytes }
  generation           uint64         副本实例代数，初始为 1
  format_version       uint32         本地元数据格式版本
```

```text
ChunkReplica
  chunk_id            ChunkId
  generation          uint64
  node_id             NodeId
  state               ALLOCATED | WRITING | HEALTHY | CORRUPT | DELETING
  size                 uint64
  checksum             Checksum
  last_verified_at     int64?
```

`generation` 用于以后区分同一逻辑 Chunk 的旧副本和重建副本，防止延迟到达的删除或完成请求误操作新副本。第一阶段固定为 `1`，但接口必须传递并校验它。

校验和类型不要写死在接口中：

```text
Checksum
  algorithm           CRC32C | SHA256
  value               bytes
```

第一版建议提交时至少计算 SHA-256；若实现依赖暂未确定，可先实现算法枚举和长度校验，再选择一个小型、经过验证的库。不要自行编写加密哈希算法。CRC32C 后续可用于传输块的快速校验，但不能替代持久化 Chunk 的强校验。

### 3.3 UploadSession

`UploadSession` 属于 Coordinator，用于组织跨节点的整个对象上传；不要把它与 Storage Node 上单个 Chunk 的临时写入句柄混为一体。

```text
UploadSession
  upload_id           UploadId
  object_id           ObjectId
  target_version      uint64
  expected_size       uint64
  chunk_size          uint32
  state               PREPARING | UPLOADING | COMMITTING |
                      COMPLETED | ABORTED | EXPIRED
  parts               vector<UploadPart>
  created_at          int64
  expires_at          int64
  session_revision    uint64         Coordinator 侧并发更新版本
```

```text
UploadPart
  index               uint32
  chunk_id            ChunkId
  expected_size       uint64
  expected_checksum   Checksum?
  generation          uint64
  targets             vector<NodeId>
  completed_replicas  vector<NodeId>
```

Storage Node 只持久化完成当前 Chunk 所需的 `ChunkWriteSession`：`upload_id`、`chunk_id`、`generation`、期望大小/校验和、已确认持久化偏移、创建时间和过期时间。

## 4. 单节点 ChunkStore

### 4.1 磁盘布局

第一阶段使用普通文件系统，不引入数据库或自研存储引擎。所有临时目录、最终目录和回收站必须位于同一文件系统，确保 `rename` 的原子性。

```text
<root>/
├── chunks/
│   └── ab/cd/<chunk-id>/
│       ├── data
│       └── meta
├── staging/
│   └── <chunk-id>.<upload-id>.<generation>.tmp/
│       ├── data.part
│       └── upload.meta
├── trash/
└── quarantine/
```

`ab/cd` 取自 Chunk ID 前四个十六进制字符，避免单目录文件过多。所有路径都由经过严格解析的 ID 生成，禁止把用户提供的 object key 直接拼入文件路径。

`meta` 和 `upload.meta` 使用小型、带 `format_version` 的有界元数据格式。第一版可采用严格的 `key=value` 文本格式，字段只包含受限 ASCII ID、十进制整数和十六进制校验和；未知必需字段或超限文件必须拒绝。暂不为了元数据引入数据库。

### 4.2 写入与断点续传

建议写入状态机：

```text
ABSENT -> WRITING -> VERIFYING -> COMMITTED
             |            |
             +-> ABORTED <-+
```

写入流程：

1. `BeginWrite` 校验 ID、generation、大小上限和校验和格式，并取得该 `chunk_id + generation` 的进程内互斥锁。
2. 若最终 Chunk 已存在且大小、generation、校验和都一致，直接返回 `AlreadyCommitted`，视为幂等成功；任一字段冲突则返回 `Conflict`。
3. 创建专属 staging 目录和 `upload.meta`，使用 `O_CREAT | O_EXCL` 防止覆盖其他尝试。
4. 数据只能按连续偏移追加。`Append(offset, data)` 要求 `offset == received_bytes`；偏移不符时返回当前持久化偏移，客户端据此恢复。
5. 每个 HTTP 请求结束时执行一次 `Checkpoint`：先 `fdatasync(data.part)`，再原子更新 `upload.meta` 中的 `durable_bytes`。不要对每个网络小包执行 fsync。
6. 进程崩溃后，只承诺 `durable_bytes` 以内的数据可续传。文件若更长，恢复时截断到该位置；文件若更短，则将进度回退到实际长度。
7. 恢复校验和时重新扫描已持久化前缀，不持久化哈希库内部状态，避免格式和版本耦合。

同一 Chunk 的并发重复写入只允许一个活动 writer。第一阶段使用进程内按 Chunk 分片的锁即可；不支持多个 Storage Node 进程共享同一数据目录。

### 4.3 校验和原子提交

`Commit` 的成功语义是：完整数据和最终元数据已经持久化，并且重启后可见。

1. 检查 `received_bytes == expected_size`。
2. `fdatasync(data.part)`，流式计算并验证最终校验和。
3. 将文件重命名为 staging 目录内的 `data`。
4. 写入最终 `meta.tmp`，执行 fsync，重命名为 `meta`。
5. 删除只属于续传阶段的 `upload.meta`，然后 fsync staging 目录，确保最终目录只包含 `data` 与 `meta`。
6. 再次检查最终路径。如果已存在，验证其内容：一致则删除本次临时目录并返回幂等成功；冲突则隔离临时目录并返回 `Conflict`。
7. 将整个 staging 目录原子 rename 为最终 Chunk 目录，然后 fsync 最终父目录。
8. 只有第 7 步完成后才能向调用方返回成功，并由 Storage Node 向 Coordinator 报告副本完成。

以“包含 data 与 meta 的目录”为提交单位，可以避免两个独立文件先后 rename 时暴露不完整组合。跨文件系统 rename 必须在初始化时检测并拒绝。

### 4.4 读取

- `OpenRead` 只打开 `COMMITTED` Chunk，先验证 meta、generation、文件大小和请求范围。
- 返回持有 fd 的 RAII `ChunkReader`；通过 `ReadAt(offset, buffer, length)` 或顺序读取向调用方提供数据。
- 读取接口不返回整个 `std::string`，也不依赖网络 `Buffer`。
- 普通读取进行轻量结构校验；完整 checksum 校验由显式 `Verify` 或后台巡检执行，避免每次下载重复扫描整个 Chunk。
- Chunk 不可变，因此打开后的 reader 与并发删除可以通过 Unix 已打开 fd 的语义安全完成。

### 4.5 删除

删除应幂等，并校验 generation：

1. 不存在视为成功。
2. generation 不一致返回 `StaleGeneration`，不得删除较新的副本。
3. 将最终 Chunk 目录原子 rename 到 `trash/`，fsync `chunks` 与 `trash` 的父目录。
4. 返回成功后异步递归清理 trash；进程崩溃只会留下可安全清理的垃圾目录。

Chunk ID 不复用是避免“延迟删除新数据”的第一道保证，generation 是第二道保证。

### 4.6 启动恢复

`Recover()` 在开始对外服务前完成轻量扫描：

- `chunks/`：校验目录名、meta 可解析、data 存在且大小一致；异常项移入 `quarantine/`，不自动当成健康副本。
- `staging/`：解析 `upload.meta`，根据 `durable_bytes` 截断或回退 `data.part`；未过期项恢复为可续传状态，过期项移入 trash。
- `trash/`：后台清理，不阻塞服务启动。
- 最终 Chunk 启动时不做全量 checksum 扫描；采用按需验证和后续低优先级巡检。

恢复结果要形成结构化报告，供 Storage Node 日志和以后向 Coordinator 上报库存差异。

## 5. ChunkStore 接口

下面是接口形态而非最终代码。返回值应使用轻量 `Status/Result<T>`，不要用异常表达预期的冲突、偏移不匹配或不存在。

```cpp
class ChunkWriter {
public:
    virtual Result<uint64_t> Append(uint64_t offset,
                                    const void* data,
                                    size_t length) = 0;
    virtual Status Checkpoint() = 0;
    virtual Result<ChunkDescriptor> Commit() = 0;
    virtual Status Abort() = 0;
    virtual uint64_t durableBytes() const = 0;
    virtual ~ChunkWriter() = default;
};

class ChunkReader {
public:
    virtual Result<size_t> ReadAt(uint64_t offset,
                                  void* output,
                                  size_t length) = 0;
    virtual const ChunkDescriptor& descriptor() const = 0;
    virtual ~ChunkReader() = default;
};

class ChunkStore {
public:
    Result<std::unique_ptr<ChunkWriter>> BeginWrite(
        const BeginChunkWrite& request);
    Result<std::unique_ptr<ChunkWriter>> ResumeWrite(
        const ResumeChunkWrite& request);
    Result<ChunkStat> Stat(const ChunkKey& key) const;
    Result<std::unique_ptr<ChunkReader>> OpenRead(
        const ChunkKey& key, const ByteRange& range) const;
    Status Delete(const ChunkKey& key);
    Result<VerifyResult> Verify(const ChunkKey& key, VerifyMode mode);
    Result<RecoveryReport> Recover();
};
```

关键约束：

- `ChunkKey = {chunk_id, generation}`，所有变更接口都带 generation。
- `BeginChunkWrite` 同时携带 `upload_id`、期望大小、校验和和过期时间。
- Writer/Reader 为 RAII 对象，负责 fd 和临时资源释放。
- HTTP 层负责状态码、Header、认证和连接背压；ChunkStore 只返回领域错误。
- ChunkStore 的同步接口可以先由磁盘线程池调用。未来改用异步 I/O 时，上层接口语义不变。

建议的领域错误至少包括：`NotFound`、`AlreadyCommitted`、`Conflict`、`OffsetMismatch`、`ChecksumMismatch`、`StaleGeneration`、`NoSpace`、`Corrupt`、`IoError` 和 `InvalidArgument`。

## 6. 通信接口规划

### 6.1 Coordinator 控制面（后续 gRPC）

- `RegisterNode`：节点 ID、地址、容量、能力和启动 epoch。
- `Heartbeat`：容量、负载、健康状态和最近错误。
- `BeginUpload`：创建 Object/version 与 UploadSession，返回分块及节点计划。
- `ReportChunkCommitted`：Storage Node 在本地提交成功后报告 `{upload, chunk, generation, node, size, checksum}`；必须幂等。Client 可以触发状态查询，但不能作为副本持久化成功的权威来源。
- `CommitUpload`：确认所有必需 Chunk 后原子发布对象版本。
- `AbortUpload`：终止会话并安排临时数据清理。
- `GetObjectPlan`：返回对象版本、Chunk 列表和可读副本地址。
- `ReportInventory`：后续用于重启对账和副本恢复，不在第一阶段实现。

节点注册和心跳由 Storage Node 的 gRPC client 发起；Coordinator 不依赖 Reactor。以后 Coordinator 下发删除、复制或校验任务时也走控制面，不把这些逻辑塞入数据面 HTTP handler。

### 6.2 Storage Node HTTP 数据面

第一版可以只支持已知长度的请求，不必立即实现 HTTP chunked transfer encoding：

```text
PUT    /v1/chunks/{chunk_id}       写入或续传 Chunk
HEAD   /v1/chunks/{chunk_id}       查询已提交状态或持久化偏移
GET    /v1/chunks/{chunk_id}       下载，支持 Range
DELETE /v1/chunks/{chunk_id}       内部管理接口，后续需要节点认证
```

上传请求携带 `upload_id`、`generation`、总大小、checksum 和 `Content-Range`。偏移不匹配时返回当前 durable offset；完全重复且内容描述一致时返回幂等成功。下载响应携带 size、checksum、generation 和标准 Range Header。

以后可以增加短期签名 token，使 Client 按 Coordinator 的计划直接访问节点；认证不是本阶段实现内容，但接口不要依赖浏览器 session 或原文件管理业务字段。

### 6.3 Client 流程

上传：

```text
BeginUpload -> 本地切分/计算校验和 -> 并发 PUT Storage Nodes
            -> ReportChunkCommitted -> CommitUpload
```

断点续传时，Client 先查询 Coordinator 的 UploadSession，再向目标节点 `HEAD` 获取持久化偏移，从该偏移继续传输。Coordinator 只接收小型状态消息。

下载：

```text
GetObjectPlan -> 按 Chunk/Range 直接 GET Storage Nodes
              -> 校验 Chunk -> 按逻辑 offset 组装对象
```

## 7. 现有 HTTP/Buffer 的大文件适配评估

当前实现不能安全地直接承载真正的大文件上传和下载，原因如下：

1. `HttpContext::processBody` 把完整请求体追加到 `HttpRequest::body()` 的 `std::string`，内存占用与 Chunk 大小线性增长。
2. `HttpServer` 只在完整请求到达后调用业务回调，无法边收边写磁盘。
3. `HttpResponse` 以完整字符串保存 body，不能表示文件流或异步 Range 响应。
4. `TcpConnection` 的输出 `Buffer` 可持续增长；虽然已有高水位回调，但缺少完整的暂停/恢复读取和低水位续传机制。
5. 跨线程 `send` 会复制数据，适合小响应，不适合不断复制大文件块。
6. 当前没有请求行/Header 总大小限制、body 最大值、上传空闲超时和慢客户端保护。
7. 当前不支持 chunked transfer encoding。第一阶段可要求 `Content-Length + Content-Range`，不把实现 chunked 作为前置条件。

进入 HTTP 数据面实现阶段前，建议做以下小范围扩展：

- 为解析器增加 `onHeaders`、`onBodyData(const char*, size_t)`、`onMessageComplete` 流式回调，body 数据被消费后立即从 `Buffer` 移除。
- 增加严格的 Header/body 限制和上传超时；流式上传期间禁止同连接 pipelining。
- 为 `TcpConnection` 增加明确的 `startRead/stopRead`，用磁盘任务队列和输出高/低水位实施背压。
- 下载采用固定大小窗口：磁盘线程 `pread` 一块，投递到 I/O Loop，写完成后再读取下一块；不要一次读完整 Chunk。
- 为发送接口增加 `size_t` 长度和所有权清晰的缓冲块。`sendfile`、mmap、io_uring 可在正确性稳定后评估，不是第一阶段要求。
- HTTP handler 只把数据片段交给 `ChunkWriter`，不直接操作 staging 路径或持久化元数据。

现有 `Buffer::readFd` 的 readv 机制可以继续使用；问题不在 Buffer 的基本结构，而在上层没有及时消费数据和建立背压。

## 8. 分阶段开发顺序

1. **单节点存储核心**：实现 common model、ChunkStore、原子提交、重复写入、删除、校验和启动恢复；用临时目录和故障点测试，不接网络。
2. **Storage Node 流式 HTTP**：扩展 HTTP body streaming 和连接背压，接入 ChunkStore，完成单节点 PUT/HEAD/GET/DELETE 与 Range 测试。
3. **最小 Coordinator**：定义 Protobuf；实现节点注册/心跳、内存中的 UploadSession、单副本放置和对象提交。持久化仓库通过接口隔离，之后再选择简单可靠的实现。
4. **Client**：实现分块、并行上传、偏移恢复、下载组装和端到端校验。
5. **元数据持久化与对账**：保证 Coordinator 重启恢复，引入 Storage Node inventory 上报和垃圾回收。
6. **多副本**：扩展放置策略、replica 状态机、复制任务和读取故障转移。
7. **恢复与重平衡**：节点失联判定、缺副本修复、限速迁移和 generation 防陈旧任务。
8. **I/O 优化**：基于指标再评估 sendfile、批量校验、磁盘队列、缓存或 io_uring。

第一阶段完成标准应是：在不启动任何网络服务的情况下，ChunkStore 的幂等写入、断点恢复、校验失败、原子提交、删除和进程重启恢复都能通过确定性的文件系统测试。
