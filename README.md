# oboe

用 [Odin](https://odin-lang.org/) 编写的高性能日志服务，存储与查询架构参照
[VictoriaLogs](https://github.com/VictoriaMetrics/VictoriaLogs)，并附带：

- 进程内分片 LRU + TTL 缓存（HTTP API）
- beanstalkd 兼容的内存任务队列（TCP 协议）

单个二进制同时提供日志写入/查询、Web UI、Prometheus 指标、缓存和队列服务。

更多文档：[架构说明](docs/architecture.md) · [测试指南](docs/testing.md)。

## 快速开始

```bash
odin build src -out:oboe
./oboe -data ./data
```

默认监听：

- HTTP `0.0.0.0:9428`（写入、查询、UI、指标、缓存）
- 队列 `127.0.0.1:11300`（beanstalkd 协议）

写入与查询：

```bash
curl -X POST http://127.0.0.1:9428/insert/jsonline \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"_time":"2026-10-26T04:00:00Z","level":"error","service":"api","_msg":"boom"}\n'

# 裸词匹配 _msg；field:value 匹配普通字段
curl 'http://127.0.0.1:9428/select/logsql/query?query=level%3Aerror&limit=100'
curl 'http://127.0.0.1:9428/select/logsql/query?query=boom'
curl 'http://127.0.0.1:9428/select/logsql/query?query=*%20%7C%20stats%20by%20(service)%20count()%20as%20total'
```

`_msg` 与 `_time` 是保留字段名；接入其他格式时可用 `_msg_field`、`_time_field` 指定映射。
普通字段（如 `level`、`service`）保持原字段名，可通过 `_stream_fields` 选择构成 stream 的字段。

浏览器打开 `http://127.0.0.1:9428/` 使用内置 Web UI。

## 命令行参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-data` | `./data` | 数据目录 |
| `-listen` | `0.0.0.0:9428` | HTTP 监听地址 |
| `-workers` | `8` | HTTP 工作线程数 |
| `-retention` | `30` | 保留天数，`0` 表示永久 |
| `-block-rows` | `8192` | 每个存储块的行数 |
| `-merge-threshold` | `8` | 分区内 part 数达到该值时合并 |
| `-stream-fields` | `level,service` | 构成 stream 的字段，逗号分隔 |
| `-flush-interval` | `1s` | memtable 落盘间隔 |
| `-cache-size` | `256MB` | 缓存容量，支持 `KB/KiB/MB/MiB/GB/GiB`，`0` 为不限 |
| `-cache-max-entries` | `1000000` | 缓存 key 数量上限，`0` 为不限 |
| `-cache-max-value` | `16MB` | 单个 value 大小上限 |
| `-no-cache` | - | 关闭缓存 |
| `-queue-listen` | `127.0.0.1:11300` | 队列监听地址；`off` 或空串关闭 |
| `-queue-max-jobs` | `1000000` | 内存中最大 job 数 |
| `-queue-max-job-bytes` | `65535` | 单个 job body 上限 |
| `-no-queue` | - | 关闭队列 |

## 存储架构

写入路径：

```
ingest (JSON line / ES bulk / Loki push)
  -> memtable（按 UTC 天分区、按 stream 分组）
  -> flush：生成不可变 part
  -> 后台合并 / 过期分区删除
```

磁盘布局：

```
data/
  partitions/
    20261026/
      parts/
        p_0000000000000007/
          meta.json     # 行数、时间范围、stream 数、block 数、块行数
          index.bin     # 每块的 stream、时间范围、行数、数据/bloom 偏移
          data.bin      # zlib 压缩的列式块
          bloom.bin     # 每块字段值 bloom filter
          merged_from   # 合并产物标记，记录被替换的 part id
```

- 每个 block 按 stream 分组、列式编码：时间戳单独成列，字符串列按行存长度表 +
  数据区，整列相同则只存一份常量；整块再做 zlib 压缩。
- 查询时先用 block 时间范围、stream 字段和 bloom filter 裁剪，再解码候选块。
- 每个 part 不可变；满足 `merge-threshold` 后做 k 路归并，合并结果写完并落盘后才替换旧 part。
- 启动时从 `p_<16位十六进制>` 目录名恢复 part 序号，读取 `merged_from` 去重，
  避免重启后序号复用导致的覆盖写入或重复计数。
- 保留策略按天删除过期分区。

## LogsQL 子集

过滤器：

- `*` 全部；裸词匹配 `_msg`
- `"exact phrase"` 短语
- `field:value`、`field:="exact"`、`field:value*` 前缀、`field:*` 存在
- `field:>value` `>=` `<` `<=` 数值/字符串比较
- `_time:[start, end]`、`_time:>timestamp`、`_time:5m`
- `NOT expr`、`-expr`、`(a OR b)`、`in(a, b, c)`

管道：

```
| limit N
| sort by (field1, field2 desc) [asc|desc]
| fields a, b, c
| stats [by (f1, f2)] count() as total, sum(x) as s, avg(x), min(x), max(x)
```

HTTP 查询参数：`query`、`limit`、`start`、`end`。
响应为 NDJSON，并返回 `X-Oboe-Scanned-Rows`、`X-Oboe-Matched-Rows`、
`X-Oboe-Pruned-Blocks`、`X-Oboe-Elapsed-Ms` 便于观察裁剪效果。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/insert/jsonline` | 每行一个 JSON 对象 |
| `POST` | `/insert/elasticsearch/_bulk` | Elasticsearch bulk |
| `POST` | `/insert/loki/api/v1/push` | Grafana Loki push |
| `GET` | `/select/logsql/query` | 查询，NDJSON |
| `GET` | `/select/logsql/field_names` | 字段名及出现次数 |
| `GET` | `/select/logsql/field_values` | 字段值及出现次数 |
| `GET/PUT/DELETE` | `/cache/<key>` | 缓存读写删 |
| `GET` | `/cache/stats` | 缓存统计 |
| `GET` | `/metrics` | Prometheus 指标 |
| `GET` | `/health` | 健康检查，返回 `OK` |
| `GET` | `/` | 内置 Web UI |

写入参数：`_time_field`、`_msg_field`、`_stream_fields`。
查询参数：`query`、`limit`、`start`、`end`。

### 缓存 API

```bash
# 写入，ttl 可选（如 30s、5m；省略表示不过期）
curl -X PUT --data-binary 'hello' 'http://127.0.0.1:9428/cache/greeting?ttl=30s'

# 读取：命中返回 200 + 原始字节 + X-Cache: hit；未命中 404 + X-Cache: miss
curl -D- http://127.0.0.1:9428/cache/greeting

curl -X DELETE http://127.0.0.1:9428/cache/greeting
curl http://127.0.0.1:9428/cache/stats
```

实现要点：

- 16 个分片，每片独立互斥锁；容量按分片均分。
- LRU 淘汰，读取时惰性清理过期项。
- 值以副本存储，读取返回独立副本，避免调用方释放后悬垂。
- 仅驻留内存，重启后为空。
- `/metrics` 暴露 `oboe_cache_entries/bytes/hits_total/misses_total/evictions_total/expirations_total`。

## 任务队列（beanstalkd 协议子集）

默认监听 `127.0.0.1:11300`，可直接使用 beanstalkd 客户端。
支持的命令：

```
put use watch ignore reserve reserve-with-timeout
delete release bury touch
peek peek-ready peek-delayed peek-buried
kick kick-job pause-tube
stats stats-tube stats-job list-tubes list-tube-used list-tubes-watched quit
```

示例：

```bash
printf 'put 0 0 60 5\r\nhello\r\nreserve-with-timeout 1\r\n' | nc 127.0.0.1 11300
```

语义要点：

- job 状态：`READY`、`RESERVED`、`DELAYED`、`BURIED`；tube 按优先级 + FIFO 序号出队。
- `delay`、`TTR`、`release`/`touch`、超时重入 ready、`pause-tube`、`kick` 均已实现。
- 客户端断开时，其 reserved job 自动回到 ready。
- `stats` / `stats-tube` / `stats-job` 返回 beanstalkd 风格 YAML。
- job 只存内存，进程退出即丢失；无持久化与主从复制。
- 队列指标：`oboe_queue_jobs/ready/reserved/delayed/buried/connections/puts_total`。

## 并发模型

- HTTP：`accept` 主循环 + 固定 worker 池，keep-alive，单连接顺序处理。
- 存储：读多写少，分区锁 + 后台 flush/merge/retention 线程。
- 缓存：请求线程直接分片加锁。
- 队列：后台线程每 50ms 推进延迟 job、处理 TTR 超时；每个连接一个线程，
  `reserve` 在 100ms 切片内等待条件变量，避免忙轮询。

## 构建与测试

需要 Odin `dev-2026-10` 或更新版本。

```bash
odin build src -out:oboe
odin test src                        # 40 个用例
odin test src -define:ODIN_STRESS=1  # 追加 3 个并发压力用例
```

测试覆盖工具函数（大小/时长/时间解析、缓冲区与编码）、zlib/gzip 压缩、JSON 行解析、
bloom filter、块编解码、存储落盘与合并后重启一致性、LogsQL 解析与执行、HTTP 路由与
三种写入协议、缓存 TTL/LRU/容量限制/并发、队列优先级/延迟/TTR/bury/kick/pause/断线回收，
共 40 个用例且无内存泄漏；`-define:ODIN_STRESS=1` 另加存储（并发写入 + 并发 flush/merge
+ 并发查询）、缓存和队列的并发压力用例。详见 [测试指南](docs/testing.md)。

测试使用 `/tmp/oboe_*` 作为临时目录，结束后自动清理；可与现有数据目录并存。

## 基准测试

`bench/` 是独立的压测驱动，覆盖写入（含落盘）、查询、缓存和队列：

```bash
odin run bench -- -mode all                          # 全量，默认 20 万行 / 8 线程
odin run bench -- -mode ingest -rows 1000000         # 1 输入 + 落盘吞吐
odin run bench -- -mode query -rows 200000 -reps 20  # 四种查询形态
odin run bench -- -mode cache -ops 100000 -threads 8
odin run bench -- -mode queue -rows 200000 -threads 8
```

单机 macOS arm64 实测（20 万行、8 线程，数据仅供量级参考）：

| 场景 | 优化前 | 优化后 |
| --- | --- | --- |
| 写入（memtable） | 4.8M rows/s | 4.8M rows/s |
| 写入 + 落盘 | 174k rows/s | 约 500k rows/s |
| `level:error \| limit 100` | 2.3k q/s（0.43 ms） | 5.2k q/s（0.19 ms） |
| `level:error \| stats by (service)` | 248 q/s（4.0 ms） | 407 q/s（2.5 ms） |
| `* \| sort by (_time) desc \| limit 100` | 37 q/s（27 ms） | 107 q/s（9.3 ms） |
| 缓存混合读写 | 9.2M ops/s | 8.7M ops/s（未改动，波动） |
| 队列 put/reserve/delete | 374k jobs/s | 444k jobs/s |

本轮优化集中在：块编码按最终列宽预分配（去掉逐行补齐）、bloom 构建去重与块内重复值
记忆化、解码单遍统计、查询侧去掉字段名 intern、`sort` 改为 limit 大小的 top-N 堆、
队列负载拷贝移出全局锁、缓存惰性读取时钟。

## 已知限制

- 无鉴权、TLS、多租户；默认 HTTP 监听所有网卡，暴露公网前请置于反向代理之后。
- LogsQL 与 beanstalkd 均为常用子集，不保证与上游完全兼容。
- 缓存与队列不持久化，也不做集群复制。
- 合并为单线程后台任务，写入量极大时可考虑按分区并行。
