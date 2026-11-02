# 使用手册（cookbook）

面向使用者的操作手册：每种能力都给出可直接复制运行的 `curl` 或 Odin 片段，
参数、返回值和限制以当前实现为准（包名 `common`、`netio`、`logdb`、`cache`、
`queue`、`web`、`cmd`）。

建议阅读顺序：

1. [日志写入](logs-ingest.md) — 三种写入协议、字段映射、批量与限制
2. [LogsQL 查询](logsql.md) — 过滤器、管道、时间范围、响应头
3. [HTTP API](http-api.md) — 端点总表、状态码、CORS、指标、UI
4. [缓存](cache.md) — HTTP 缓存与库内 API、TTL、快照、容量语义
5. [任务队列](queue.md) — beanstalkd 子集、客户端片段、worker 模式
6. [嵌入到程序](embedding.md) — 各组件最小片段、生命周期、所有权、并发
7. [运维](operations.md) — 数据目录、保留/合并、备份恢复、安全与限制

其他文档： [架构说明](../architecture.md) · [包布局](../modules.md) ·
[库用法](../library.md) · [测试指南](../testing.md)。

约定：

- 示例假设服务以默认参数启动：HTTP `0.0.0.0:9428`，队列 `127.0.0.1:11300`。
- `curl` 中的查询串用 `--data-urlencode`，避免 `|`、空格、`:` 手工转义出错。
- 库示例从仓库内运行；仓库外使用 `-collection:oboe=/path/to/oboe`，导入写成
  `import "oboe:logdb"`。
