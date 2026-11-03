# Storage Node V1 遗留问题与 Coordinator V1 开发边界

本文汇总 Storage Node V1 验收发现的问题，以及 [HTTP 数据面文档](03_storage_node_http.md)中的已知限制。基于当前源码、测试和 [单节点数据服务规范](../specs/001-harden-storage-node/spec.md)整理；它是后续开发的待办清单，不能代替故障注入或断电验证。此处的 P0/P1/P2 是问题严重度；`spec.md` 中用户故事的 Priority 不是同一套分级。

## 当前结论与优先顺序

**可以开始 Coordinator V1 的独立开发**，但不应把 Storage Node V1 标记为已满足 `spec.md` 的全部验收标准。开始时应限定为可信网络、单进程独占一个本地存储 root、已知长度的单请求连接，并在 Coordinator 中保留对结果不确定、节点 I/O 故障和请求重试的处理空间。两条曾列为 P0 的问题均发生在 `fsync` 失败或操作中断后的收口路径；按当前用途暂列 **P1**。这不表示其持久化风险已经消失。

建议按以下顺序推进：

1. Coordinator V1 的控制面与单节点对接可以先做；遇到 `503`、`X-Commit-State: unknown` 或节点 I/O 错误时，查询状态并隔离有问题的节点，不能把错误直接解释为未提交，也不能盲目重试写入或删除。
2. 在宣称 `201`/`204` 的崩溃后持久化语义、将删除用于可靠的副本迁移，或完成 Storage Node 严格验收之前，修复下文 **S1、S2**，并补定向故障注入与重启测试。
3. 在提高节点数量或接入持续真实负载之前，优先处理资源回收、请求头边界、超时语义及容量上限，并完成相应压力与故障测试。

## P0：目前没有确认的待修复项

此前验收提出的 root 排他和正常提交/恢复目录同步问题已修复，见 [P0 修复报告](03_storage_node_http.md#p0-修复报告root-排他与目录持久化)。测试包含同 root 多进程竞争、定向 `fsync` 失败和恢复；普通 Debug 与 ASan/UBSan 两套 CTest 在上次修复验收时均为 **8/8 通过**。这些测试不等于真实断电试验。下表中的 S1、S2 是修复之后仍未覆盖的更窄的失败收口问题，不能与已修复项混淆。

## P1：可靠性和功能待办

| 编号 | 触发场景与影响 | 代码位置 | 建议修复与验证 |
| --- | --- | --- | --- |
| **S1：删除后的源目录同步失败** | `Delete` 把 final 目录移到 `trash` 后，若同步 final 的父目录失败或操作在此时中断，重试看到 final 不存在便返回成功；HTTP 映射为 `204`。随后崩溃时，目录删除可能未持久化，旧 Chunk 可能再次出现。这是调用顺序推导，尚无断电复现。 | [`ChunkStore::Delete`](../storage/chunk_store/ChunkStore.cc#L346)、[`MoveDirectory`](../storage/chunk_store/ChunkStoreInternal.cc#L340)、[`StorageNode::StartDelete`](../storage/node/StorageNode.cc#L584) | 对“已移动但未确认同步”的状态给出可重试/可查询语义；避免在未确认源父目录持久化时返回成功。定向注入源父目录 `fsync` 失败，核对即时重试、重启恢复和 HTTP 状态。需要检查持续 EIO，不能无限盲目重试。**可靠删除用于副本迁移前必须解决。** |
| **S2：新建分片祖先目录同步失败** | 首次创建 `chunks/<前缀>/<前缀>` 时，若同步其父目录失败，目录可能已经可见。再次进入 `EnsureDirectory` 会因目录存在直接返回 OK；恢复发布 final 后只同步 final 的直接父目录，可能报告提交成功，但新建的分片目录项未确认持久化。这同样是故障路径推导，未做断电复现。 | [`EnsureDirectory` / `EnsureFinalParent`](../storage/chunk_store/ChunkStoreInternal.cc#L242)、[`LoadCommitted`](../storage/chunk_store/ChunkStoreInternal.cc#L298)、[`ChunkStore::Recover`](../storage/chunk_store/ChunkStore.cc#L395) | 对已存在但上次创建同步失败的祖先目录重新确认父目录持久化；覆盖正常提交及恢复路径。定向注入新分片父目录 `fsync` 失败，再重试/重启，核对不能提前报告已提交。**严格 `201` 持久化验收前必须解决。** |
| **H1：请求头总量限制可绕过** | 16 KiB 计数使用解析后的 `path` 与 headers map；query 和被覆盖的重复头不计入。长 query 或大量重复头可以超过声明上限，占用解析/连接内存。 | [`StorageNode::OnMessage`](../storage/node/StorageNode.cc#L138)、[`HttpRequest::addHeader`](../net/HttpRequest.h#L126) | 按原始请求行与头部字节累计，在消费之前拒绝超限；测试长 query、重复头和跨多次读取的请求头。 |
| **H2：非法请求头可被当成头部结束** | `HttpContext` 将任何没有冒号的非空行走入“空行”分支；畸形请求可能进入业务处理，余下字节的边界也会错位。 | [`HttpContext::processHeaders`](../net/HttpContext.cc#L54) | 只接受真正的 `\r\n` 作为头部终止；其他无冒号行返回解析错误，并补 socket 测试。 |
| **N1：空闲超时不按实际进展判断** | `lastActivity` 仅在消息回调及整个输出缓冲排空时更新。持续发生部分写、但长期不排空的下载，以及耗时较长的正常磁盘工作，仍可能被定时器关闭。 | [`StorageNode::OnConnection` 定时器](../storage/node/StorageNode.cc#L99)、[`OnMessage`](../storage/node/StorageNode.cc#L138)、[`OnWriteComplete`](../storage/node/StorageNode.cc#L558) | 明确“无进展”的定义和磁盘任务超时策略；按读/写进展更新时间，测试慢客户端、部分写、慢磁盘及真正空闲连接。 |
| **N2：连接上限检查晚于连接对象创建** | `TcpServer` 已创建并登记 `TcpConnection`，Storage Node 回调才拒绝超额连接。连接突发时，fd 和连接对象可短时超过配置上限。 | [`TcpServer::newConnection`](../net/TcpServer.cc#L77)、[`StorageNode::OnConnection`](../storage/node/StorageNode.cc#L99) | 在更早的接入位置做容量控制或证明可接受的瞬时上界；测试饱和时新连接的拒绝时延和现有连接的响应性。避免为此引入通用网络框架重构。 |
| **N3：致命 socket 读错误未关闭连接** | `readFd` 返回非 `EAGAIN`/`EWOULDBLOCK`/`EINTR` 错误时只记录并调用 `handleError`，没有完成关闭；连接可能滞留到超时，或反复触发可读事件。 | [`TcpConnection::handleRead`](../net/TcpConnection.cc#L227) | 对致命读错误进入现有关闭路径，验证回调与 `Session` 资源释放；以可控故障测试或代码级测试覆盖。 |
| **D1：废弃数据无回收规则** | `Delete`、`Abort` 与恢复可能把目录移到 `trash`/`quarantine`，当前没有物理回收流程。持续删除、校验失败或过期上传会占满磁盘。 | [`ChunkStore::Delete` / `Recover`](../storage/chunk_store/ChunkStore.cc#L346)、[HTTP 文档已知限制](03_storage_node_http.md#已知限制) | 定义保留期、安全条件和可重复回收流程；同时处理过期 staging，并验证不会删除健康 Chunk、活动 Writer 或未过期可续传数据。对应 `FR-014`、`SC-007`。 |
| **H3：checksum 失败响应中的偏移易误导** | 完整数据 Checkpoint 后若 `Commit` 校验失败，`Abort` 成功将 staging 移走，但 `422` 响应仍带完整的 `X-Durable-Offset`。调用方可能将其误认为可续传偏移。 | [`StorageNode::SubmitUploadWindow`](../storage/node/StorageNode.cc#L347) | `Abort` 成功时不返回可续传偏移，或明确标明该上传尝试已失效；测试错误 checksum 后的响应和 HEAD。 |
| **C1：提交/中止中间失败后的结果不确定** | `Commit` 或随后的 `Abort` 在中途失败时，HTTP 返回 `503` 与 `X-Commit-State: unknown`，但当前没有保证所有失败都能在同一进程内立即收敛。客户端若直接重传可能冲突或误判。 | [`SubmitUploadWindow`](../storage/node/StorageNode.cc#L347)、[HTTP 协议说明](03_storage_node_http.md#put上传一个连续片段) | 保留现有显式 unknown 语义；Coordinator 先通过 HEAD 查询，必要时待节点恢复后再决策。按关键故障点补注入测试，区分可立即查询与需重启恢复的状态。 |

## P2：优化及当前阶段可接受的限制

| 项目 | 当前状态与代价 | 后续处理条件 |
| --- | --- | --- |
| **每窗口同步与重复目录同步** | PUT 每个 64 KiB 窗口 `Checkpoint`；`LoadCommitted` 在 GET/HEAD 等路径会同步 final 父目录。可靠性语义清晰，但可能限制吞吐。 | 先建立吞吐/延迟基线，再评估批量同步；任何优化都应保留故障恢复语义。 |
| **额外用户态复制** | 上传和下载使用固定窗口与 `std::string`/`Buffer` 传递，当前不采用零拷贝。 | 只有基准测试显示复制是瓶颈时再优化。 |
| **重复 PUT 不校验重传 body** | 已提交 Chunk 的重复 PUT 按描述符判断幂等，仍读完请求体，但不重新计算 body checksum。这是现有协议约束。 | Coordinator 必须使用稳定的描述符和请求标识；若以后要求逐字节校验重复请求，再修改协议与测试。 |
| **没有后台全量 checksum 巡检** | 普通 GET 只做轻量结构检查，并返回存储的 checksum；静默数据损坏需要客户端下载后校验或显式 Verify 才能发现。 | 接入多副本和修复流程时再决定巡检频率与成本。 |
| **在途磁盘任务不可强行取消** | 断线或有序关闭后，已提交线程池的操作继续完成，丢弃响应并释放对象；这也是保留可续传前缀的当前策略。 | 如资源占用或关停时延成为实测问题，再定义安全取消点。 |
| **协议与部署范围** | 当前仅支持 `Content-Length`、单请求连接、单段 Range；无 chunked、keep-alive、pipelining、TLS、认证或请求签名。 | Coordinator V1 按现有协议调用；只在可信网络使用，暴露到不可信网络前另行设计访问控制。 |
| **旧设计文档可能过时** | [一期设计文档](01_object_storage_phase1_design.md)保留早期方案；HTTP 协议与实现以 [当前 HTTP 文档](03_storage_node_http.md)及源码为准。 | 后续涉及接口变更时核对旧文档，不把历史计划写成已实现能力。 |

## 尚未通过测试证明的验收能力

现有集成测试覆盖 8 MiB 上传/下载、闭区间与后缀 Range、无效 Range、断线续传、重启、8 个不同 Chunk 的并发请求、慢读客户端、模拟慢磁盘、活动上传与 DELETE 冲突、关停和空闲连接；ChunkStore 有部分定向 `fsync` 故障注入。测试通过只说明这些具体场景可行，不能推导出所有边界条件都正确。

与 [`spec.md`](../specs/001-harden-storage-node/spec.md) 相比，以下仍缺少对应的完整验证：64 MiB 与零字节 Chunk、开放结尾 Range、16 并发且重复 10 次、连接与任务队列饱和、请求头畸形和超限、相同 Chunk 的更多操作交错、HTTP 层磁盘故障及下载中途读失败、部分网络读写和关闭回调交错、慢速但持续有进展的连接、删除/过期数据回收、S1/S2 的同步失败与恢复。`SC-001` 至 `SC-007` 不能仅凭现有 8/8 测试宣称全部达成；`FR-014` 的回收规则明确尚未实现。

下一阶段宜先完成 Coordinator V1 的最小节点登记和状态查询链路，并将上述条目作为独立任务跟踪。每项关闭时记录触发条件、修复后的协议或持久化语义、定向测试与剩余限制。
