审查结论已收敛：基础请求路径和常见断线恢复有测试支撑，但“确认持久化后能抗异常断电”、root 独占和全局容量上限尚不能验收为可靠。下一步我会把已复现的问题、代码推断的风险和单纯缺测试的项分别列出，并据此给出进入 Coordinator V1 的条件。
## 验收结论

Storage Node V1 的常规数据路径可用，但目前不能作为已验收的 Coordinator V1 数据面基础。 已有测试验证了正常上传下载、部分 Range、续传、慢读和一次有序退出；持久化故障、root 独占及容量饱和等关键保证仍缺失或未验证。

我重新构建了普通与 ASan/UBSan 配置，两套现有 CTest 均 8/8 通过。另用临时 root 和真实 socket 复现了两项问题：两个服务进程能同时使用同一 root；约 20 KiB 的请求行绕过了声明的 16 KiB 头部限制。本轮未修改业务代码。

### P0：进入下一阶段前必须处理

| 问题与证据 | 触发场景、影响 | 建议 |
|---|---|---|
| 同一 root 没有进程间排他。 Initialize (storage/chunk_store/ChunkStore.cc:355)只检查目录和文件系统；实测两个进程均成功启动。 | 两进程的活动 Writer 锁互不知晓，可并发操作相同 Chunk，破坏单节点一致性前提。 | 启动时取得覆盖服务生命周期的 root 排他锁；增加双进程验收测试。 |
| 确认持久化的目录边界尚不完整。 BeginWrite (storage/chunk_store/ChunkStore.cc:434)创建 staging 子目录后未见同步其父目录；Checkpoint (storage/chunk_store/ChunkWriter.cc:78)同步数据与子目录；Commit (storage/chunk_store/ChunkWriter.cc:152)跨目录发布后只同步目标父目录。 | 在异常断电的文件系统故障模型下，已返回的续传偏移或提交结果可能缺少所需的目录项持久化保证。这是代码推断的风险，现有测试未复现断电丢失。 | 明确文件系统与成功语义，补齐必要的父目录同步，并对每个同步、重命名失败点做故障注入及恢复验证。 |

### P1：可靠性与协议问题

| 问题与位置 | 触发场景、影响 | 建议 |
|---|---|---|
| 请求头上限漏算 query；无冒号的头行会被当作头部结束。StorageNode.cc (storage/node/StorageNode.cc:242)、HttpContext.cc (net/HttpContext.cc:54) | 20 KiB query 的请求行实测被接受并处理为 404，违反 16 KiB 限制；畸形头行也可能被解释为 body 起点。 | 按实际收到的完整请求行与头部字节计数，拒绝畸形头行；补边界测试。 |
| 连接上限并非接入阶段的硬上限。TcpServer.cc (net/TcpServer.cc:77)先创建并登记连接，StorageNode.cc (storage/node/StorageNode.cc:197)再异步强制关闭超额连接。 | 大量同时接入时，可暂时积累超过 256 个连接对象和待执行关闭任务；尚无饱和测试证明内存上界。 | 在接入路径限制未处理连接数量，并测试突发连接、任务队列满及恢复。 |
| 校验失败且 Abort 成功后，错误响应仍附带旧的持久化偏移。StorageNode.cc (storage/node/StorageNode.cc:470) | 客户端收到 422 和完整的 X-Durable-Offset，但暂存目录已移走；随后的 HEAD 可能是 404，续传提示自相矛盾。 | 对已废弃上传不返回可续传偏移；分别测试 422、Abort 失败和提交结果未知。 |
| 超时只在收到消息或整块写完时刷新。StorageNode.cc (storage/node/StorageNode.cc:209)、OnWriteComplete (storage/node/StorageNode.cc:643) | 极慢但持续有进展的客户端，或耗时较长的磁盘任务，可能被判为空闲；现有测试只覆盖完全空闲连接。 | 定义“进展”的计时规则，并测试慢上传、慢下载与长磁盘操作。 |
| 非暂时性 TCP 读取错误仅记录日志。TcpConnection.cc (net/TcpConnection.cc:227) | 连接可能继续占用资源或重复触发错误事件。此项由代码路径推断，尚无注入复现。 | 对不可恢复错误关闭连接，并覆盖复位、读错误与资源释放。 |
| trash 没有清理；过期 staging 只在启动恢复时处理。Recover (storage/chunk_store/ChunkStore.cc:674)、Delete (storage/chunk_store/ChunkStore.cc:598) | 长期运行会持续占用磁盘，最终影响正常写入。 | 制定安全回收规则，验证不会清理活动或未过期上传。 |

### P2：可暂缓

- ChunkWriter::Checkpoint (storage/chunk_store/ChunkWriter.cc:78)随每个 64 KiB 上传窗口执行同步，可能限制吞吐；应先测量再调整窗口或检查点频率。
- 通用 TcpConnection 输出缓冲 (net/TcpConnection.cc:137)没有硬容量限制；Storage Node 当前靠“上一窗口写完再读下一窗口”控制正常下载路径。若以后改变发送调度，须重新证明上界。

## 已验证的能力与测试缺口

PUT 不聚合完整 body，GET 按窗口读取；暂停接收、有界磁盘队列和单连接一个在途任务构成了有效的正常路径背压。StorageNode.cc (storage/node/StorageNode.cc:414) 与现有集成测试 (tests/integration/storage_node_test.py:120)支持这一结论。读取中的短 pread 可在后续窗口继续读；读取失败后会断开连接，使客户端可通过响应长度识别不完整下载。普通 GET 不重新核算 SHA-256，客户端必须校验返回的数据。

测试尚未覆盖 64 MiB 与 16 连接的规格目标、队列和连接饱和、开放结尾 Range 与零字节 Chunk、下载中途读失败、真实磁盘错误及提交各故障点、异常断电、多进程竞争和多次生命周期交错。ASan/UBSan 通过不能替代这些场景，也没有证明不存在所有并发竞态。

## 与 spec.md 的核对及下一步

spec.md (specs/001-harden-storage-node/spec.md:11)的 Current Baseline 基本符合代码；其 FR 和 SC 是目标验收条件，不是当前能力声明。其中 FR-013（root 独占）已实测不满足，FR-014（回收）尚未实现；FR-010 的头部上限存在反例。SC-001 至 SC-006 所要求的大规模传输、饱和、故障注入和重复并发运行均尚未通过现有测试。HEAD 查询暂存偏移还需要提供 X-Upload-Id，规划测试时应保留这个现有前提。

建议先处理两项 P0，并完成对应故障与双进程测试；随后修正协议边界和错误响应，补齐饱和及生命周期测试。可以着手设计 Coordinator V1 的独立规格，但不宜把当前 Storage Node V1 当作已通过持久化与并发验收的节点实现来集成。