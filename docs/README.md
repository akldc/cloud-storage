# 文档阅读顺序

编号表示建议阅读顺序；历史设计保留其写作时点的状态描述，当前实现请以源码、README 和最新阶段文档为准。

| 顺序 | 文档 | 内容 |
| --- | --- | --- |
| 00 | [C++11 特性笔记](00_cpp11_features.md) | 通用语言学习资料，可按需阅读 |
| 01 | [对象存储一期设计](01_object_storage_phase1_design.md) | 最初的系统边界和数据模型；部分 HTTP 评估已过时 |
| 02 | [ChunkStore 与 Storage Node 学习指南](02_ChunkStore_StorageNode_Guide.md) | 已实现单节点源码与执行流程 |
| 03 | [Storage Node HTTP 数据面](03_storage_node_http.md) | 当前 Chunk API、流式传输与关闭语义 |
| 04 | [Storage Node V1 遗留问题](04_storage_node_v1_issues.md) | 当前未解决的可靠性和功能边界 |
| 05 | [Coordinator V1 开发设计](05_Coordinator_V1_Development.md) | 节点管理和候选放置的开发设计；对应能力现已实现 |
| 06 | [单副本对象上传下载设计](06_Single_Replica_Object_Development.md) | 阶段 A 至 E 的基础链路与验收已完成；可选 Abort 待开发 |
