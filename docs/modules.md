# 包布局与模块边界

oboe 的源码按职责拆成多个 Odin 包。目录名就是包名，包内使用相对导入，仓库外可以
用 collection 导入。`cmd/` 是唯一可执行入口，其余都是库包。

## 包布局

| 包 | 目录 | 职责 |
| --- | --- | --- |
| `common` | `common/` | 基础类型与无依赖工具：`Arena`、`Buf`、时间/大小/时长解析、FNV、zlib/gzip 包装 |
| `netio` | `netio/` | 缓冲 TCP 行读取、监听地址解析 |
| `logdb` | `logdb/` | 日志存储、JSON 行解析与 stream id、LogsQL 解析与执行、part/block/bloom/merge/retention |
| `cache` | `cache/` | fastcache 风格桶式缓存、TTL、大值 FIFO、可选文件快照 |
| `queue` | `queue/` | beanstalkd 兼容的内存 job/tube 状态机、文本协议与连接管理 |
| `web` | `web/` | HTTP/1.1 服务（nbio 事件循环 + 业务线程池）、路由、写入/查询/缓存/指标端点、内嵌 UI |
| `cmd` | `cmd/` | 命令行参数解析、组件装配、信号处理与退出清理 |

Elasticsearch bulk 与 Loki push 的协议解析在 `web/handlers.odin`：解析后调用
`logdb.parse_json_line`/`compute_stream_id` 生成 `Ingest_Row`，再写 store。
`logdb` 本身不依赖 HTTP。非库目录：`examples/`（可运行示例）、`bench/`（压测驱动）、
`tests/`（跨包端到端与压力用例）、`docs/`、`scripts/`（CI）。

## 依赖方向

```
common  ←  netio  ←  queue
common  ←  logdb
common  ←  cache
common  +  netio  +  logdb  +  cache  +  queue  ←  web  ←  cmd
```

箭头指向被依赖的包。没有包级循环，`cmd` 只负责装配。

实际的 import 关系（以源码为准）：

| 包 | 允许导入 |
| --- | --- |
| `common` | 仅标准库/vendor（`vendor:zlib`） |
| `netio` | `common` |
| `logdb` | `common` |
| `cache` | `common` |
| `queue` | `common`、`netio` |
| `web` | `common`、`netio`、`logdb`、`cache`、`queue` |
| `cmd` | `common`、`logdb`、`cache`、`queue`、`web` |

## 新代码放哪

| 要加的东西 | 放哪 |
| --- | --- |
| 新的时间/大小/时长解析、缓冲区、压缩包装 | `common` |
| TCP 行协议、监听地址处理 | `netio` |
| 新的存储格式、JSON 行解析、LogsQL 语法/执行、part/block/bloom 格式 | `logdb` |
| 缓存淘汰策略、TTL、快照格式 | `cache` |
| job 状态机、beanstalkd 命令、队列统计 | `queue` |
| HTTP 路由、ES bulk/Loki push 协议适配、指标、UI | `web` |
| 新的命令行参数、组件装配/关闭顺序 | `cmd` |

测试放在 `tests/`（跨包端到端）或对应包的 `*_test.odin`（包内单元测试）。不要为了
测试在库包里加只有测试会调用的公开入口。

## 禁止反向依赖

- `common` 不能导入任何其他业务包。
- `netio` 不能导入 `logdb`、`cache`、`queue`、`web`。
- `logdb`、`cache` 不能导入 `netio`、`queue`、`web`、`cmd`，也不能互相导入。
- `queue` 不能导入 `web` 或 `cmd`。
- 库包不能导入 `cmd`；`cmd` 里的 `main` 不属于任何库。
- `web` 可以依赖所有库包，但库包不能反过来依赖 `web`。

如果新功能需要跨层数据，优先让拥有数据的包导出最小接口，或由 `web`/`cmd` 在装配
层做适配；不要把业务类型塞进 `common` 来绕过依赖方向。

## public API 一览

各库包的包级声明默认对其他包可见（库包当前没有 `@(private)`）；`cmd` 内部用
`@(private)` 隐藏信号处理状态。下表是跨包稳定契约，未列出的符号视为内部实现，
完整签名以源码为准。

### `common`

`Arena`（`arena_alloc/arena_str/arena_reset/arena_destroy`）、`Buf`
（`buf_init/buf_destroy/buf_reset/buf_write/buf_string/buf_addf/buf_time_rfc3339`）、
`now_ns`、`utc_day`、`parse_time_ns`、`parse_iso_ns`、`parse_duration_ns`、
`parse_size_bytes`、`zlib_compress`、`zlib_decompress`、`inflate_any`。

### `netio`

`Conn`、`conn_fill`、`conn_read_byte`、`conn_read_exact`、`conn_read_line`、
`parse_listen`，以及 `netio/chunked.odin` 里的 `chunk_frame` / `Chunk_Framer`。

`chunk_frame` 是 HTTP/1.1 chunked 帧的唯一实现：它把「从请求头结束处的缓冲区 +
累计已解码量 + `max_body` + 单行上限」推进成 `Complete`（含 body 结束偏移）、
`Need_More`、`Bad_Format`、`Too_Large` 四种结果，可选地把 payload 追加到输出缓冲。
放在 `netio` 而不是 `web`，是因为事件循环（`web/nbio_server.odin`）与请求解析器
（`web/http.odin`）都必须调用它，而 `netio` 不依赖 HTTP 层，两边都能导入；如果把
它留在 `web`，循环侧和解析侧仍会各写一份状态机，这正是要消除的重复。

`Conn` 有两种字节来源：默认从 `sock` 阻塞读取；当 `Conn.src` 非空时改为从
预填充切片读取。HTTP 事件循环把完整请求交给业务线程后，业务线程用后者复用同
一套解析函数，解析期间不接触网络。`netio` 本身不感知 HTTP 或并发模型，仍只
依赖 `common`。

### `web`

`web/http.odin` 只做协议：状态机、上限判定、响应构造。`web/nbio_server.odin`
拥有 `core:nbio` 事件循环与连接缓冲，负责 accept、请求完整性判定、把完整请求
投递给业务线程、异步写回以及关停时序。`-workers` 只决定业务线程数量；连接数上限
是独立参数 `-max-conns`，越限连接在循环线程内直接回 `503` 并关闭，不进入
`Http_Conn` / 任务队列，也不计入活跃连接统计。两者的边界是 `Http_Conn.raw`
（循环侧请求字节）和响应缓冲区（业务侧分配、发送回调释放）。

### `logdb`

| 类别 | 入口 |
| --- | --- |
| 存储 | `default_store_config`、`store_init`、`store_close`、`store_ingest`、`store_flush_due`、`store_flush_all`、`store_merge_all`、`store_apply_retention`、`store_stats` |
| 写入解析 | `Parsed_Line`、`Field`、`parse_json_line`、`compute_stream_id`、`Ingest_Row` |
| 查询 | `parse_query_string`、`query_destroy`、`execute_query`、`Query_Result`、`result_destroy`、`out_row_get` |
| 常量 | `DEFAULT_QUERY_LIMIT`（1000）、`MAX_QUERY_LIMIT`（1,000,000）、`MAX_JSON_FIELDS`（128） |

### `cache`

`Cache_Config`、`default_cache_config`、`Cache`、`cache_init`、`cache_destroy`、
`cache_set`、`cache_get`、`cache_get_into`、`cache_delete`、`cache_stats`、
`cache_save_file`、`cache_load_file`、`Cache_Saver`、`cache_saver_run`。

### `queue`

`Queue_Config`、`default_queue_config`、`Queue`、`queue_init`、`queue_destroy`、
`queue_put`、`queue_reserve_into`、`queue_delete`、`queue_release`、`queue_bury`、
`queue_touch`、`queue_kick`、`queue_kick_job`、`queue_pause_tube`、
`queue_peek_into`、`queue_stats`、`queue_tube_stats`、`queue_job_stats`、
`queue_list_tubes`；连接相关 `queue_new_conn`、`queue_conn_opened`、`queue_use`、
`queue_watch`、`queue_unwatch`、`queue_conn_closed`；服务端
`queue_server_start`、`queue_server_stop`。

### `web`

`Server_Config`、`default_server_config`、`Server`、`server_init`、`server_run`、
`server_request_stop`、`server_stop`、`server_destroy`、`Http_Request`、
`Http_Response`、`dispatch`。

### `cmd`

`package main`，不可被库使用者导入；只含 `main` 与内部参数解析，不对外提供稳定 API。

## 为什么 `logdb` 暂不再细拆

`logdb` 内部按文件分了 `store.odin`、`part.odin`、`block.odin`、`bloom.odin`、
`jsonline.odin`、`queries.odin`、`execute.odin`，但保持同一个包，原因是：

- 这些文件共享大量内部类型和热路径数据结构（`Row`、`Field`、`Part_Meta`、
  `Block_Index`、`Decoded_Block`、`Filter`/`Pipe` 等）。拆成子包就必须把它们
  导成 public API，或者复制一份，容易产生两套不一致的格式定义。
- 查询执行同时读取 part、block、bloom 和 memtable；拆包会让这些路径跨包调用，
  增加接口层与锁边界，收益低于维护成本。
- 磁盘格式和查询语义需要一起演进；单包内改 `Part_Meta`/`Block_Index` 时可以一次
  改完写入、读取和测试。

如果以后要拆，建议先稳定磁盘格式并在 `logdb` 内抽出一个明确的只读接口，再把
写入与查询拆成子包；不要为了目录好看而提前拆。
