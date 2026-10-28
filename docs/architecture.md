# oboe 架构说明

本文描述 oboe 的模块划分、写入与查询生命周期、磁盘格式和并发不变量。面向需要修改
存储、查询或协议实现的开发者；日常使用请看 [README](../README.md)。

## 1. 进程结构

单个 `oboe` 进程由四个相互独立的子系统组成，共用进程内存与数据目录：

```
main
 ├─ Store          日志存储（memtable / part / merge / retention）
 ├─ Cache          16 分片 LRU + TTL 缓存
 ├─ Queue          beanstalkd 兼容任务队列
 └─ HTTP Server    accept 循环 + worker 池，路由到 ingest/query/cache/metrics/UI
      └─ Queue Server（独立 TCP 端口，每连接一个线程）
```

`main.odin` 只负责解析命令行、装配组件和优雅关闭；业务逻辑都在各模块中，HTTP
`dispatch` 不依赖真实 socket，因此可以在测试里直接调用。

### 源码地图

| 文件 | 职责 |
| --- | --- |
| `main.odin` | 命令行参数、组件创建/销毁、版本号 |
| `http.odin` | TCP accept、worker 池、HTTP/1.1 请求解析与响应、超时 |
| `handlers.odin` | 路由、三种写入协议、查询/字段名/字段值、缓存 HTTP API、Prometheus 指标 |
| `jsonline.odin` | JSON 行解析、`_time`/`_msg` 提取、stream id 计算 |
| `queries.odin` | LogsQL 词法/语法分析、时间范围合并 |
| `execute.odin` | 查询执行、块裁剪、过滤、sort/stats/fields/limit |
| `store.odin` | 分区、memtable、flush、merge、retention、启动恢复 |
| `part.odin` | part 目录的写入/读取、`index.bin`、`meta.json` |
| `block.odin` | 列式块编解码 |
| `bloom.odin` | 每块 bloom filter 的构建与查询 |
| `compress.odin` | zlib/gzip 压缩、解压与大小上限 |
| `cache.odin` | 分片 LRU + TTL 缓存 |
| `queue.odin` | job/tube 状态机与堆 |
| `queue_server.odin` | beanstalkd 文本协议与连接管理 |
| `util.odin` | 时间/大小解析、缓冲区、arena、编码辅助 |
| `ui.odin` / `ui.html` | 编译期内嵌的 Web UI |

## 2. 写入路径

### 2.1 三种入口

所有入口最终都产生 `Ingest_Row{ts, stream, msg, fields}` 并调用 `store_ingest`。

- **JSON line**：每行一个 JSON 对象。`_time`（或 `_time_field`）解析为时间戳，`_msg`
  （或 `_msg_field`）作为消息，其余键作为普通字段。嵌套对象/数组保留原始 JSON 文本。
  一个对象最多保留 128 个字段（`MAX_JSON_FIELDS`），重复键以后出现的值为准。
- **Elasticsearch bulk**：跳过 action 行，只写入文档行；默认把 `message` 映射为
  `_msg`、`@timestamp` 映射为 `_time`，可通过 `_msg_field`/`_time_field` 覆盖。
- **Loki push**：`streams[].stream` 的标签成为普通字段；stream id 优先使用配置的
  stream 字段，若 `-stream-fields` 为空则使用全部标签。
  `values[i][0]` 为纳秒字符串（不超过 13 位时按秒处理），`values[i][1]` 为消息。
  成功返回 `204`。

写入参数：`_time_field`、`_msg_field`、`_stream_fields`。stream id 由配置的 stream
字段按固定顺序做 FNV 哈希，字段顺序不影响结果；未配置 stream 字段时 id 为 0。

### 2.2 memtable 与 flush

- `store_ingest` 在写锁内把行复制进对应 **UTC 天分区** 的 arena，时间戳 `<= 0` 时取
  当前时间。
- 后台线程每 250 ms 检查一次 flush 条件：行数达到 `flush_max_rows`（20 万）、估算
  字节数达到 `flush_max_bytes`（128 MiB），或最旧一行等待超过 `flush_interval`
  （默认 1 s）。
- flush 时先在锁内把 memtable 换成新的空表，再在锁外按 `(stream, ts)` 排序并写入新
  part。写入失败会把行重新放回 memtable。
- part 目录名为 `p_<16 位十六进制序号>`，序号由原子计数器分配；启动时从已有目录名
  恢复最大序号，避免重启后覆盖。

## 3. 磁盘格式

```
data/
  partitions/
    20261026/
      parts/
        p_0000000000000007/
          meta.json
          index.bin
          data.bin
          bloom.bin
          merged_from      # 仅合并产物有
```

### 3.1 `meta.json`

`Part_Meta` 字段：`version`、`id`、`rows`、`blocks`、`min_ts`、`max_ts`、
`data_size`、`bloom_size`、`index_size`、`created_ns`。`id` 即目录名。

### 3.2 `index.bin`

固定 12 字节头 + 每条 64 字节的块记录：

```
0   "ODLI"
4   version:u16 (=1)
6   record_size:u16 (=64)
8   nblocks:u32
12  records[nblocks]
```

每条记录：

| 偏移 | 字段 | 类型 |
| --- | --- | --- |
| +0 | stream | u64 |
| +8 | min_ts | i64 |
| +16 | max_ts | i64 |
| +24 | rows | u32 |
| +28 | raw_len（解压后字节） | u32 |
| +32 | comp_len | u32 |
| +36 | 保留（写 0） | u32 |
| +40 | data_off | u64 |
| +48 | bloom_off | u64 |
| +56 | bloom_len | u32 |
| +60 | 保留（写 0） | u32 |

### 3.3 `data.bin` 与块编码

每个块是一个独立的 zlib 流。解压后的列式布局：

```
"ODLB" | version:u16 | nrows:u32 | ncols:u32 | stream:u64 | min_ts:i64 | max_ts:i64
timestamps: nrows * i64
每列:
  name_len:u16 | name
  kind:u8
    0 = 常量:  len:u32 | value
    1 = 逐行:  lens[nrows]:u32 | 拼接的 value 数据
```

- 一个块只包含同一个 stream 的行，块内行数不超过 `block_rows`（默认 8192）。
- `_msg` 是特殊列；空字符串表示该行没有该字段，解码时不会生成空字段。
- 整列值相同时使用常量编码；整块随后再压缩。
- 编码先按行收集各列值（列宽在首行确定后一次分配，缺失字段保持空串）；解码时扫描
  长度表一遍即完成校验、字段计数与前缀和，再按列填充，避免逐行临时分配。
- `ODLI`/`ODLB` 是格式魔数，属于磁盘格式标识，不是项目名称，改名时刻意保留。

### 3.4 `bloom.bin`

每个块一份 bloom filter，直接按 `bloom_off/bloom_len` 定位：

- 7 个哈希位置；最小 1 KiB，最大 32 KiB（`BLOOM_MAX_BITS`），按唯一 token 数约
  10 bit/项向上取 2 的幂。
- 构建时索引两种 key：消息/字段值中的 **token（大小写折叠）**，以及不超过 512
  字节的 **完整值（不折叠）**。
- 同一块内重复的字段值会命中构建器的直接映射记忆表（`Bloom_Memo`，256 槽，按
  完整值哈希索引并逐字节确认相等），直接复用已算好的哈希列表；重复哈希在收尾时
  用开放寻址表去重，filter 大小按唯一项数决定。
- 查询时 `WORD` 用折叠 token 检查，`EXACT` 用完整值检查；`PHRASE`、`PREFIX`、
  `RANGE` 无法用 bloom 裁剪，直接扫描。

### 3.5 `merged_from`

合并产物在替换旧 part 之前写入该文件，每行一个被替换的 part id。启动时若旧目录
尚未删除，会根据所有 marker 去掉被取代的 part，防止崩溃后重复计数或复活旧数据。

## 4. 查询路径

### 4.1 解析

`queries.odin` 的词法器识别括号、字符串、`field:`、比较符、`|` 等；解析器构建
`Filter` 树和 `Pipe` 列表。支持：

- 过滤：`*`、裸词（匹配 `_msg`）、短语、`field:value`、`field:="exact"`、
  `field:value*`、`field:*`、比较、`_time` 范围、`NOT`/`-`、`OR`、`in(...)`。
- 管道：`limit`、`sort`、`fields`/`keep`、`stats`。

`_time` 条件和 HTTP `start`/`end` 参数会合并成查询级边界：起点取最大值，终点取
最小值。时间表达式支持 RFC3339、Unix 秒/毫秒/微秒/纳秒和相对时长（如 `5m`）。

### 4.2 执行顺序

`execute_query` 在 store 的共享读锁内完成一次扫描：

1. 跳过与查询时间范围不相交的 **分区**（按 UTC 天）。
2. 跳过与范围不相交的 **块**（`min_ts/max_ts`）。
3. 对 `WORD`/`EXACT` 过滤器检查块 bloom，不匹配则计入 `pruned_blocks`。
4. 解压候选块，逐行检查时间边界和完整过滤树。
5. 扫描完磁盘 part 后，再扫描该分区的 memtable（未落盘数据同样可见）。

管道行为：

- 无 `sort`、无 `stats` 时，达到 `limit` 立即停止扫描。
- `sort` 维护一个大小为 `limit` 的最大堆（top-N）：凑满后只有比堆顶更靠前的行才会
  替换堆顶，结束时对堆内行排序输出，避免先把全部匹配行留在内存里。
- `stats` 必须扫描完所有匹配行；`limit` 只限制输出分组数。没有 `by` 且无匹配时
  仍返回一行零值聚合。
- `fields` 在排序/聚合之后用于裁剪输出字段。

响应为 NDJSON，`_time` 输出 RFC3339，`_msg` 与普通字段按原样转义。响应头：

- `X-Oboe-Scanned-Rows`
- `X-Oboe-Matched-Rows`
- `X-Oboe-Pruned-Blocks`
- `X-Oboe-Elapsed-Ms`

## 5. 合并与保留

### 5.1 合并

后台线程每 10 s 检查一次：分区内 part 数达到 `merge_threshold`（默认 8）时触发。

- 若所有 part 总行数不超过 `merge_max_rows`（400 万），全部参与合并；否则挑选
  `merge_threshold` 个最小的 part。
- 使用每个 part 的游标做 k 路归并，顺序为 `(stream, ts)`。
- 新 part 写完后写 `merged_from`，再在独占锁内替换 part 列表；离开锁后删除旧目录。
- 合并期间读查询持有共享锁，因此不会看到被删除 part 的悬垂指针。

### 5.2 保留

每 60 s 执行一次：`cutoff = 今天(UTC) - retention_days`，删除 `day < cutoff` 的
整个分区（含 memtable 与 part 目录）。`retention_days = 0` 关闭过期删除。

### 5.3 启动恢复

- 只打开能完整读取 `meta.json`、`index.bin`、`bloom.bin`、`data.bin` 的 part；
  不完整的 part 目录会被删除。
- 读取所有 `merged_from`，剔除已被取代的 part。
- 从目录名恢复序号计数；新 flush/merge 不会复用已加载的序号。
- 启动时不会重建 memtable：未 flush 的数据在进程退出时由 `store_close` 写入磁盘。

## 6. 缓存

- 16 个分片，`FNV(key) % 16` 选片；每片独立互斥锁和 LRU 双向链表。
- 总容量和条目上限按分片均分（向上取整）。
- `GET` 命中会触碰 LRU；过期项在读取时惰性删除并计入 `expirations`。
- 写入超限时从 LRU 尾部淘汰；值在写入和读取时各复制一次，调用方拿到的始终是独立
  副本。
- 条目可能由任意线程写入、由任意线程淘汰，因此 key/value/entry 统一使用
  `cache_init` 时捕获的 allocator 分配和释放。
- key 上限 1024 字节，value 上限由 `-cache-max-value` 控制；仅驻留内存。

## 7. 任务队列

- tube 内 `READY` 用按 `(priority, seq)` 的小顶堆；`DELAYED` 用按
  `(delay_until, seq)` 的堆；全队列 `RESERVED` 用按 `(deadline, id)` 的堆。
- 后台线程每 50 ms 推进：延迟到期的 job 进入 ready，TTR 超时的 reserved job 重新
  ready 并增加 `timeouts`。
- `reserve` 在没有可用 job 时按 100 ms 切片等待条件变量，直到超时或被唤醒，避免
  忙轮询。每个客户端连接一个线程，多个连接可以同时阻塞在 reserve。
- `release` 可重新设置优先级和延迟；`touch` 延长当前 deadline；`bury` 进入
  buried；`kick`/`kick-job` 将其移回 ready；`pause-tube` 期间该 tube 不再出队。
- 连接断开时，该连接 reserved 的 job 全部回到 ready。
- job 只存内存，受 `max_jobs`、`max_job_bytes` 限制；无持久化和复制。

## 8. HTTP 并发模型

- 主循环 `accept`，把连接放入队列（上限 2^18）并唤醒 worker；默认 8 个 worker。
- 一个连接由固定 worker 顺序处理，支持 HTTP/1.1 keep-alive。
- 支持 `Content-Length`、chunked、`Expect: 100-continue`、请求体 gzip 解压。
- 上限：请求头 64 KiB、请求体 256 MiB、空闲读超时 60 s、发送超时 30 s；
  开启 `TCP_NODELAY`。响应带宽松 CORS 头和 `Server: oboe`。
- `/health` 返回 `OK`，`/metrics` 输出 Prometheus 文本格式。

## 9. 并发不变量

1. **part 不可变**：只有合并会生成新 part；旧 part 在独占锁内从列表移除，之后才
   删除文件，因此持有读锁的查询不会读到已释放内存。
2. **memtable 只在锁内读写**；flush 在锁外写盘，失败可安全重放。
3. **查询持共享锁完成整个扫描**，flush/merge 的元数据变更持独占锁。
4. **stream id 稳定**：相同 stream 字段和值在任何进程、任何顺序下得到相同 id。
5. **缓存条目所有权统一**：分配与释放使用缓存自己的 allocator，与调用线程无关。
6. **队列只依赖自身锁**：HTTP 线程只读队列统计，不参与 job 状态迁移。

## 10. 可观测性

`/metrics` 当前暴露：

| 组 | 指标 |
| --- | --- |
| 写入/存储 | `oboe_ingested_rows_total`、`oboe_ingested_bytes_total`、`oboe_flushed_parts_total`、`oboe_merged_parts_total`、`oboe_parts`、`oboe_partitions`、`oboe_mem_rows`、`oboe_rows`、`oboe_disk_bytes` |
| 查询/HTTP | `oboe_queries_total`、`oboe_http_requests_total`、`oboe_http_connections_total`、`oboe_uptime_seconds` |
| 缓存 | `oboe_cache_entries`、`oboe_cache_bytes`、`oboe_cache_hits_total`、`oboe_cache_misses_total`、`oboe_cache_evictions_total`、`oboe_cache_expirations_total` |
| 队列 | `oboe_queue_jobs`、`oboe_queue_ready`、`oboe_queue_reserved`、`oboe_queue_delayed`、`oboe_queue_buried`、`oboe_queue_connections`、`oboe_queue_puts_total` |

查询裁剪效果优先看响应头，不进入指标，避免高基数标签。
