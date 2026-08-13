# Ragent AI 项目架构分析

> 本文档基于 `E:\projects\ragent` 仓库源码（最新提交 `020e5c3`，2026-08-12）分析整理，
> 覆盖前后端整体架构、模块依赖关系、核心链路设计与各模块详细实现。

## 文档目录

| 文档 | 内容 |
|:---|:---|
| [01-总体架构设计.md](./01-总体架构设计.md) | 项目定位、技术栈、总体架构、部署形态、数据模型概览 |
| [02-模块依赖关系与依赖链.md](./02-模块依赖关系与依赖链.md) | 7 个 Maven 模块依赖矩阵、完整依赖链、编译/运行/数据流三类依赖 |
| [03-后端模块详解.md](./03-后端模块详解.md) | framework / infra-ai / system / bootstrap / mcp-server / agent 各模块设计与实现 |
| [04-RAG核心链路与关键机制详解.md](./04-RAG核心链路与关键机制详解.md) | rag 模块（460 个 Java 文件）的聊天流水线、混合检索、入库内核、意图路由、限流、取消、Trace 等核心机制 |
| [05-前端架构详解.md](./05-前端架构详解.md) | React 前端技术栈、目录结构、状态管理、SSE 流式交互、管理后台路由 |

## 一句话总结

**Ragent AI 是一个面向 Agentic RAG 的生产级 Java AI 应用平台**：后端采用
Spring Boot 4.1（Java 21）多模块单体架构（framework / infra-ai / system / rag /
agent / bootstrap / mcp-server），前端采用 React 18 + Vite + TypeScript 的
SPA，覆盖"文档入库 → 混合检索 → 意图路由 → 智能问答 → 溯源反馈"完整链路，
并提供管理后台（知识库、意图树、入库流水线、Trace、模型配置、用户与审计）。

## 关键结论速览

1. **后端是"分层单体 + 独立边车"**：7 个模块中 6 个组成一个可执行 Spring Boot
   应用（bootstrap 装配），`mcp-server` 是独立部署的 MCP 工具服务（HTTP + Streamable）。
2. **agent 模块当前只有 pom，没有实现代码**：v2 ReAct 执行架构已预留模块与配置
   （`ragent.engine.type: agent`），当前默认走 v1 workflow 编排；`agent` 配置段
   （provider/model/max-iters/max-retries）已就绪。
3. **模型层完全可插拔**：Chat / Embedding / Rerank / VLM 四类能力均抽象为
   接口 + 多 Provider 实现（百炼、Ollama、SiliconFlow、AIHubMix），带 Tier 档位、
   健康熔断（half-open）与流式首包探测自动回退。
4. **检索是"多通道并行 + 后处理链"**：向量（Milvus）、关键词（ES，可选）、
   图谱（LightRAG，可选）、联网（You.com，可选）四通道并行，RRF 融合 → 去重 →
   Rerank → 元数据富化；检索范围由意图树 + 置信度判定。
5. **入库是"固定五段内核 + 可编排节点流水线"双轨**：`IngestionKernel`
   （identity → parse → chunk → embed → index）处理文档，`IngestionEngine`
   支持用户自定义节点链（Fetcher → Parser → Chunker → Enhancer → Enricher → Indexer）。
6. **并发控制是分布式的**：聊天走 Redis ZSet 公平队列 + 可过期信号量 + Lua 原子
   抢占 + 主题通知；文档解析用分布式信号量（semaphore）限流。
7. **端到端可观测**：AOP 埋点生成 `t_rag_trace_run / t_rag_trace_node`，
   管理后台可查看每次问答的节点级 Trace；业务变更通过 bizlog 记录前后快照。

## 规模统计（代码阅读时统计）

| 维度 | 数值 |
|:---|:---|
| Java 源文件（main） | 649 |
| 前端源文件（src） | 114 |
| 数据库表（schema_pg.sql） | 24 |
| REST 接口（后端 Controller） | 约 90 个 |
| 单元测试文件 | 46 |

## 分析口径说明

- 文中"模块"指 Maven 模块（`framework`、`infra-ai`、`system`、`rag`、`agent`、
  `bootstrap`、`mcp-server`）。
- 代码路径省略公共前缀 `src/main/java/com/nageoffer/ai/ragent/`，例如
  `framework/context/UserContext.java` 表示
  `framework/src/main/java/com/nageoffer/ai/ragent/framework/context/UserContext.java`。
- 源码注释为 GBK 编码，本文档在 UTF-8 下整理，引用的类名、配置项以代码为准。
