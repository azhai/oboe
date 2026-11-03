# 基准与压测

本页给出一套可重复的基准套件，以及一轮完整矩阵的结果。所有数字都由
`bench/run_matrix.sh` 采集、`bench/summarize.py` 聚合，原始 JSONL 在 `/tmp/oboe_matrix/results/raw.jsonl`。

## 结论先行

1. **HTTP 小请求的上限由 `-workers` 直接决定**：`/health` keep-alive 下 1 个 worker 约
   6.6 万 rps，8 个 worker 约 16.4 万 rps；并发 16/64/128/512 时 `served` 恒为 8、
   其余用户全部 `stalled`，吞吐停在 14.9–16.5 万，不再增长。这是 worker-per-connection
   模型的直接读数，而不是「并发加不上去」。
2. **最大的性能短板是写入/刷盘路径**：批量 100 行的 jsonline 首轮 427 万 rows/s，
   随后两轮跌到 78 万 / 71 万；elastic bulk 339 万 → 48 万 / 40 万；loki 166 万 →
   115 万 / 62 万。8 秒持续写入后进程 RSS 峰值约 3.8 GiB、`oboe_mem_rows` 积压
   2315 万行，SIGTERM 后 33 秒才把积压刷完（主线程在 zlib 压缩）。刷盘吞吐约
   120 万 rows/s，明显低于 400 万 rows/s 的摄入速度。
3. **查询成本随数据量近似线性，唯一的例外是带 `limit` 的 filter**：20 万行时
   stats/sort/narrow 分别为 300/66/224 qps（1 线程），500 万行时降到 12.6/3.2/15.4，
   正好对应扫描行数 5 万→125 万、20 万→487 万、6.9 万→90 万。filter + `limit 500`
   在 8192 行就截断，因此 20 万与 500 万档都是 0.6 ms、约 1600 qps。
4. **并发 > 业务线程数对查询没有收益**：small 档 stats 在 conc 8 / 32 分别是
   1185 / 1320 qps，p50 6.68 / 5.87 ms；`served` 始终是 8，第 9 个用户起只是排队，
   不产生已完成请求，所以表里的 p99 不能当作排队延迟，排队延迟体现为 `stalled`。
5. **缓存小值被 HTTP 层限住，大值被内存带宽限住**：64 B 的 PUT/GET-hit/miss 都在
   15 万 ops/s 附近；8 MiB 的 PUT 913 ops/s（7.3 GiB/s）、GET-hit 1309 ops/s
   （10.5 GiB/s），miss 仍是 15 万 ops/s。
6. **队列在 8 客户端就饱和**：单连接 put/get/cycle/bury 为 61.9k/33.8k/21.6k/34.3k
   ops/s；8 客户端升到 125k/76.7k/48.5k/79.9k，32 客户端几乎不再变化（132k/79.8k/
   49.7k/78.8k），呈现单锁/单 tube 的竞争形态。
7. **长稳 185 秒没有泄漏**：RSS 10 秒基线 18.9 MiB、结束时 18.9 MiB（+0.1%），
   FD 16，`/metrics` 的 counter 全部单调；185 秒内服务 1666 万次 HTTP 请求、
   25.8 万次查询，吞吐 9.1 万 rps + 1433 qps。
8. **缺陷与异常**：写入时 memtable 会涨到 GB 级并在退出时长时间刷盘；每请求新建连接
   在 `conc ≥ 8` 时被 macOS 源端口/TIME_WAIT 限制（实测 TIME_WAIT 3.26 万，
   可用临时端口仅 16384），这组数字是客户端上限，不是服务端上限。

## 套件结构

| 文件 | 作用 |
|---|---|
| `bench/main.odin` | 子命令分发：`ingest` / `query` / `cache` / `queue-tcp` / `all` |
| `bench/support.odin` | 参数解析、线程编排、最近秩百分位、JSON/人类可读输出 |
| `bench/corpus.odin` | 固定的合成日志语料（level/service/zone/host/user/duration） |
| `bench/ingest.odin` | 播种（写 store）与播种吞吐统计 |
| `bench/flush_profile.odin` | `flush-profile`：真实刷盘/合并路径的分阶段图像、块大小分布、prof-off 对照 |
| `bench/query.odin` | 4 类 LogsQL 查询的库内压测（无 HTTP）与 scanned/matched/pruned |
| `bench/cache.odin` | 缓存 PUT/GET、命中/未命中、小值/接近上限大值 |
| `bench/queue_tcp.odin` | beanstalk 子集 TCP 客户端：put/get/cycle/bury+kick |
| `bench/loadgen/main.odin` | 正式入库的 HTTP 压测器（见下） |
| `bench/run_matrix.sh` | 完整矩阵编排：构建、播种、起服、分档压测、soak、汇总 |
| `bench/summarize.py` | 读 `raw.jsonl`，取 3 次中位数、标注方差、生成 Markdown |

## 如何运行

```bash
# 一次性构建（CI 也走这两条）
odin build bench -out:/tmp/oboe_bench -o:speed
odin build bench/loadgen -out:/tmp/oboe_loadgen -o:speed

# 完整矩阵，约 35 分钟（含 180 秒 soak）
OUT=/tmp/oboe_matrix bench/run_matrix.sh

# 冒烟（2 秒采样、2 次重复、1M 行大档、60 秒 soak）
OUT=/tmp/oboe_quick bench/run_matrix.sh --quick

# 只跑某一阶段
OUT=/tmp/oboe_matrix bench/run_matrix.sh --only small
```

矩阵脚本的环境变量：

| 变量 | 默认 | 含义 |
|---|---|---|
| `OUT` | `/tmp/oboe_matrix` | 输出根目录（`bin/`、`bodies/`、`logs/`、`results/`、`soak/`） |
| `ODIN` | `/opt/homebrew/bin/odin` | 编译器 |
| `HOST` / `PORT` | `127.0.0.1` / `19428` | 被测 HTTP 地址 |
| `QPORT` | `12399` | 被测队列端口 |
| `WRITE_PORT` / `WRITE_QPORT` | `19429` / `12398` | 写入阶段的独立进程 |
| `WORKERS` | `8` | 服务端业务线程数 |
| `WARM` / `DUR` | `2000` / `5000` | 预热/采样毫秒 |
| `REPS` | `3` | 重复次数（取中位数） |
| `SMALL_ROWS` / `BIG_ROWS` | `200000` / `5000000` | 两档数据量 |
| `SOAK_SECONDS` | `180` | 长稳时长 |

`--quick` 会把采样压到 2 秒、重复 2 次、BIG_ROWS 改为 1M、soak 改为 60 秒；
`--no-soak` 跳过长稳；`--only small|big|write|soak` 只跑该阶段。

矩阵生成的报告：`$OUT/results/summary.md`；原始记录：`$OUT/results/raw.jsonl`；
进程卡住时的强制退出记录：`$OUT/results/shutdown.log`。

## HTTP 压测器（`bench/loadgen`）

每个虚拟用户是一个 OS 线程，按测量窗口（先用 `-warm` 预热，再在 `-dur` 内采样）统计；
keep-alive 与每请求新建连接两种模式共用一套统计。

| 参数 | 默认 | 含义 |
|---|---|---|
| `-addr` | `127.0.0.1:9428` | 目标地址 |
| `-path` | `/health` | 请求目标（可带 query string） |
| `-method` | `GET`（有 body 时 `POST`） | 方法 |
| `-body` / `-body-file` | 空 | 内联或文件请求体 |
| `-conc` | 8 | 虚拟用户数（= OS 线程数） |
| `-warm` / `-dur` | 1000 / 5000 | 预热/采样毫秒 |
| `-requests` | 0 | 每用户测量请求数；非 0 时不用 `-dur` |
| `-conns` | `keep-alive` | `keep-alive` 或 `per-request` |
| `-timeout` | 2000 | socket 超时毫秒 |
| `-expect` | 200 | 期望状态码；0 表示任意 |
| `-rows-per-req` | 0 | 一个请求代表的行数，用于输出 `rows/s` |
| `-rotate` | 0 | 把 `-path` 中的 `{i}` 按该值取模展开（缓存热点环） |
| `-label` | 由 path 推导 | 输出标签 |
| `-json` | 关 | 输出单行 JSON |

JSON 关键字段：`qps`、`p50_ms`/`p95_ms`/`p99_ms`/`max_ms`、`ok`、`http_err`、
`transport_err`、`dial_err`、`served`、`stalled`、`wait_p50_ms`/`wait_p99_ms`、`bytes`、
`rows_per_s`。`served` 是窗口内至少完成一次请求的用户数，`stalled` 是其余；
正是这两个字段让「并发 > 业务线程数」可判读。

## bench 子命令

```bash
# 播种 + 库内查询 + 缓存 + 队列
oboe_bench -mode ingest  -rows 5000000 -threads 8 -batch 500 -data /tmp/d -keep
oboe_bench -mode query   -reuse -data /tmp/d -threads 1 -reps 10 -warm 2 -json
oboe_bench -mode cache   -threads 8 -ops 20000 -ops-large 1000 -value-size 64
oboe_bench -mode queue-tcp -addr 127.0.0.1:12399 -qmode cycle -clients 8 -jobs 2000
oboe_bench -mode all     -data /tmp/d
oboe_bench -mode flush-profile -data /tmp/d -profile-reps 3 -block-rows 8192
```

| 参数 | 含义 |
|---|---|
| `-mode ingest\|query\|cache\|queue-tcp\|all\|flush-profile` | 子命令；`ingest` 默认写 `-data` |
| `-rows` / `-threads` / `-batch` | 播种行数、并发线程、每批行数 |
| `-profile-reps` / `-block-rows` | `flush-profile` 每档重复次数（默认 3）、每块行数（默认 8192） |
| `-sort-threads` / `-flush-threads` | 覆盖刷盘的排序/编码线程数，0 = 按逻辑核自动；`flush-profile` 用它做串行与并行对照 |
| `-ingest-shards` | 覆盖每个分区 memtable 的分片数（独立锁），0 = 自动（逻辑核数，钳在 4..16）；`flush-profile` 用它做单锁与分片对照 |
| `-reps` / `-warm` | 每类查询的测量次数与预热次数 |
| `-ops` / `-ops-large` / `-value-size` | 缓存操作数、大值操作数、小值字节数 |
| `-data` / `-keep` / `-reuse` | 数据目录；保留；复用已有数据 |
| `-addr` / `-qmode` / `-clients` / `-jobs` / `-tube` / `-rounds` | 队列模式参数 |
| `-json` | 单行 JSON 输出 |

## 测量纪律

- 每档固定 **2 秒预热 + 5 秒采样**，每档重复 **3 次取中位数**；`*` 表示该档吞吐的
  (max−min)/中位数 > 30%，是噪声较大的读数，不应作为稳定值引用。
- 服务端与压测端跑在**同一台 10 核 M4** 上，客户端与服务端争抢 CPU 和网络栈；
  因此表中的吞吐是保守下界，单机无法区分「服务端上限」与「同机压测上限」。
- 服务端固定为 `-workers 8`、`-cache-size 512MB`、`-cache-max-value 16MB`、
  `-max-body` 默认（256 MiB）、`-retention 0`、`-flush-interval 1s`。
- 数据目录 `/tmp/oboe_matrix/data-*` 位于 APFS 内部盘（`/dev/disk3s5`，运行开始时
  29 GiB 可用）；每档播种前重建，均为空盘；仓库只放代码与报告。
- 构建均为 `-o:speed`，**未使用 `-microarch`**；Odin 版本
  `dev-2026-10:84bc3fc21`。
- 性能结论分成两件事：**吞吐上限**看 `qps`/`rows/s` 与 `served`；**延迟**看
  p50/p95/p99。慢查询（sort/stats/narrow）的 p99 主要由 `-workers 8` 的 CPU 排队与
  同机客户端竞争决定，而不是由连接数决定。

## 机器与配置

- CPU：Apple M4，10 逻辑核 / 10 物理核；内存 16 GiB；macOS 26.5.1。
- Odin：`/opt/homebrew/bin/odin version dev-2026-10:84bc3fc21`。
- 构建：`-o:speed`（服务端、bench、loadgen），无 `-microarch`。
- 服务端：`-listen 127.0.0.1:19428 -workers 8 -queue-listen 127.0.0.1:12399
  -cache-size 512MB -cache-max-value 16MB -retention 0 -flush-interval 1s`。
- 数据：`/tmp`（APFS，内部盘，非空盘但每档重建），`du`：small 1.5 MiB、big 32.5 MiB。
- `kern.ipc.somaxconn = 128`；临时端口范围 `49152–65535`（16384 个）；
  `net.inet.tcp.msl = 15000` ms。

## 结果

以下表均为 3 次重复的中位数，`*` 为方差标记。

### 数据播种

| tier | rows | ingest rows/s | flush rows/s | parts | partitions | disk MiB |
|---|---|---|---|---|---|---|
| small | 200000 | 9256688 | 984872 | 1 | 1 | 1.5 |
| big | 5000000 | 9248521 | 2083549 | 2 | 1 | 32.5 |

播种本身属于批量内存写入：约 925 万 rows/s；落盘（flush）只有 98 万 / 208 万 rows/s。

### HTTP 写入

| endpoint | rows/req | body KiB | qps | rows/s | p50 ms | p95 ms | p99 ms | errors |
|---|---|---|---|---|---|---|---|---|
| jsonline-1 | 1 | 0.2 | 140960 | 140960 | 0.05 | 0.09 | 0.12 | 0 |
| jsonline-100 | 100 | 19.2 | 7819* | 781920* | 0.14 | 0.41 | 2.35* | 0 |
| elastic-100 | 100 | 20.5 | 4806* | 480600* | 0.14 | 0.39 | 22.25* | 0 |
| loki-100 | 100 | 10.3 | 11461* | 1146100* | 0.18 | 0.28 | 1.39* | 0 |

重复明细（同一进程内连续三轮，能直接看到吞吐衰减）：

| endpoint | rep | qps | rows/s | p50 ms | p99 ms | requests |
|---|---|---|---|---|---|---|
| jsonline-1 | 1 | 122506 | 122506 | 0.06 | 0.12 | 612528 |
| jsonline-1 | 2 | 144168 | 144168 | 0.05 | 0.11 | 720842 |
| jsonline-1 | 3 | 140960 | 140960 | 0.05 | 0.13 | 704801 |
| jsonline-100 | 1 | 42673 | 4267280 | 0.14 | 0.83 | 213364 |
| jsonline-100 | 2 | 7819 | 781920 | 0.14 | 2.35 | 39096 |
| jsonline-100 | 3 | 7140 | 714000 | 0.14 | 11.28 | 35700 |
| elastic-100 | 1 | 33882 | 3388180 | 0.14 | 0.74 | 169409 |
| elastic-100 | 2 | 4806 | 480600 | 0.14 | 22.25 | 24030 |
| elastic-100 | 3 | 4022 | 402240 | 0.15 | 55.16 | 20112 |
| loki-100 | 1 | 16581 | 1658140 | 0.17 | 0.96 | 82907 |
| loki-100 | 2 | 11461 | 1146100 | 0.18 | 1.39 | 57305 |
| loki-100 | 3 | 6242 | 624200 | 0.18 | 32.00 | 31210 |

### LogsQL（HTTP `/select/logsql/query`）

| tier | class | conc | qps | p50 ms | p95 ms | p99 ms | served | errors |
|---|---|---|---|---|---|---|---|---|
| small | filter | 1 | 1586.80 | 0.60 | 0.82 | 1.10 | 1 | 0 |
| small | filter | 8 | 6179.00 | 1.25 | 2.02 | 2.42 | 8 | 0 |
| small | filter | 32 | 5658.00* | 1.32* | 2.32* | 3.12* | 8 | 0 |
| small | stats | 1 | 299.80 | 3.15 | 3.92 | 6.71* | 1 | 0 |
| small | stats | 8 | 1184.80* | 6.68* | 9.30* | 10.72* | 8 | 0 |
| small | stats | 32 | 1320.00 | 5.87* | 8.60 | 10.96 | 8 | 0 |
| small | sort | 1 | 66.20 | 14.70 | 15.93 | 20.79 | 1 | 0 |
| small | sort | 8 | 257.20* | 30.51* | 39.04* | 46.04* | 8 | 0 |
| small | sort | 32 | 290.80 | 27.18 | 35.34 | 41.27 | 8 | 0 |
| small | narrow | 1 | 224.20 | 4.32 | 5.00 | 5.30* | 1 | 0 |
| small | narrow | 8 | 770.80* | 9.71* | 16.16 | 23.80* | 8 | 0 |
| small | narrow | 32 | 802.00* | 9.70* | 14.10* | 17.95* | 8 | 0 |
| big | filter | 1 | 1624.00 | 0.60 | 0.74 | 0.99 | 1 | 0 |
| big | filter | 8 | 5498.40* | 1.35 | 2.60* | 3.04* | 8 | 0 |
| big | stats | 1 | 12.60 | 76.77 | 89.85 | 125.10* | 1 | 0 |
| big | stats | 8 | 48.60 | 162.37 | 206.46 | 222.41 | 8 | 0 |
| big | sort | 1 | 3.20 | 294.92 | 345.43 | 345.43 | 1 | 0 |
| big | sort | 8 | 12.00* | 702.40* | 764.47* | 788.77* | 8 | 0 |
| big | narrow | 1 | 15.40 | 63.54 | 70.88* | 97.95* | 1 | 0 |
| big | narrow | 8 | 46.40 | 167.66 | 205.38* | 217.88* | 8 | 0 |

查询定义：

| class | LogsQL |
|---|---|
| `filter` | `level:error \| limit 500` |
| `stats` | `level:error \| stats by (level) count() as total` |
| `sort` | `* \| sort by (_time) desc \| limit 100` |
| `narrow` | `* \| stats by (host) count() as total`，时间窗为 10 分钟数据中的最后 2 分钟 |

### LogsQL（库内直接调用，无 HTTP）

`scanned/matched/pruned` 为单次查询平均值。

| tier | class | threads | qps | p50 ms | p95 ms | p99 ms | scanned | matched | pruned | errors |
|---|---|---|---|---|---|---|---|---|---|---|
| small | filter | 1 | 2161.20 | 0.35 | 0.39 | 0.39 | 8192 | 500 | 0 | 0 |
| small | filter | 8 | 7010.80 | 0.64 | 1.65 | 2.14* | 8192 | 500 | 0 | 0 |
| small | filter | 32 | 7140.10* | 1.12 | 10.91 | 16.11* | 8192 | 500 | 0 | 0 |
| small | stats | 1 | 254.80 | 3.14 | 3.72 | 3.72 | 50000 | 50000 | 21 | 0 |
| small | stats | 8 | 1033.20 | 5.77 | 8.06 | 9.05* | 50000 | 50000 | 21 | 0 |
| small | stats | 32 | 1133.80 | 8.20* | 73.73* | 93.32* | 50000 | 50000 | 21 | 0 |
| small | sort | 1 | 55.80 | 14.72 | 15.57 | 15.57 | 200000 | 198830 | 0 | 0 |
| small | sort | 8 | 228.70 | 27.64 | 34.05 | 38.02 | 200000 | 198736 | 0 | 0 |
| small | sort | 32 | 215.30 | 116.92 | 229.99* | 281.98* | 200000 | 198544 | 0 | 0 |
| small | narrow | 1 | 191.00 | 4.34 | 4.50 | 4.50 | 68928 | 38830 | 0 | 0 |
| small | narrow | 8 | 715.10* | 8.81* | 11.76 | 14.14 | 68928 | 38736 | 0 | 0 |
| small | narrow | 32 | 784.70 | 15.61* | 92.05* | 118.17* | 68928 | 38544 | 0 | 0 |
| big | filter | 1 | 2223.70 | 0.34 | 0.40 | 0.40 | 8192 | 500 | 0 | 0 |
| big | filter | 8 | 9812.30 | 0.39 | 1.04 | 1.23 | 8192 | 500 | 0 | 0 |
| big | stats | 1 | 11.60 | 71.75 | 72.18 | 72.18 | 1250000 | 1250000 | 459 | 0 |
| big | stats | 8 | 55.50 | 117.32 | 130.04 | 137.51 | 1250000 | 1250000 | 459 | 0 |
| big | sort | 1 | 3.00 | 279.47 | 287.73 | 287.73 | 4868928 | 4862026 | 0 | 0 |
| big | sort | 8 | 14.20 | 465.63 | 508.69 | 527.05 | 4836160 | 4821192 | 0 | 0 |
| big | narrow | 1 | 15.60 | 53.11 | 53.74 | 53.74 | 904000 | 862026 | 0 | 0 |
| big | narrow | 8 | 67.80 | 94.51 | 108.70 | 117.83 | 841920 | 821192 | 0 | 0 |

### 缓存（HTTP，8 并发 keep-alive）

`MiB/s` 是值本身的吞吐（qps × 每次传输的值大小），不是含 JSON 包装的 HTTP 字节数；
miss 场景没有值传输，记 0。大值 8 MiB 接近 `-cache-max-value 16MB` 的一半。

| 场景 | payload | value B | qps | MiB/s | p50 ms | p99 ms | errors |
|---|---|---|---|---|---|---|---|
| put-small | 64 B | 64 | 149525 | 9.1 | 0.051 | 0.109 | 0 |
| get-hit-small | 64 B | 64 | 150250 | 9.2 | 0.051 | 0.107 | 0 |
| get-miss-small | 64 B | 0 | 152085 | 0.0 | 0.050 | 0.099* | 0 |
| put-large | 8 MiB | 8388616 | 913 | 7304.0 | 7.781 | 29.531 | 0 |
| get-hit-large | 8 MiB | 8388616 | 1309 | 10470.4 | 5.504 | 22.721 | 0 |
| get-miss-large | 8 MiB | 0 | 151282 | 0.0 | 0.050 | 0.109 | 0 |

### 队列（TCP beanstalk 子集）

| qmode | clients | ops/s | p50 ms | p95 ms | p99 ms | kick p50 ms | kick p99 ms | errors |
|---|---|---|---|---|---|---|---|---|
| put | 1 | 61855 | 0.015 | 0.021 | 0.051 | 0.000 | 0.000 | 0 |
| put | 8 | 125036 | 0.062 | 0.093* | 0.124* | 0.000 | 0.000 | 0 |
| put | 32 | 132578 | 0.236 | 0.284 | 0.330 | 0.000 | 0.000 | 0 |
| get | 1 | 33753 | 0.027 | 0.038 | 0.083* | 0.000 | 0.000 | 0 |
| get | 8 | 76710 | 0.101 | 0.143 | 0.171 | 0.000 | 0.000 | 0 |
| get | 32 | 79839 | 0.395 | 0.460* | 0.524* | 0.000 | 0.000 | 0 |
| cycle | 1 | 21630 | 0.043 | 0.061 | 0.133 | 0.000 | 0.000 | 0 |
| cycle | 8 | 48531 | 0.161 | 0.217 | 0.259* | 0.000 | 0.000 | 0 |
| cycle | 32 | 49694 | 0.637 | 0.733 | 0.877 | 0.000 | 0.000 | 0 |
| bury | 1 | 34252 | 0.027 | 0.036* | 0.077* | 0.029 | 0.041* | 0 |
| bury | 8 | 79918 | 0.097 | 0.136* | 0.168* | 0.051 | 0.088 | 0 |
| bury | 32 | 78758 | 0.395 | 0.457 | 0.503 | 0.182 | 0.282 | 0 |

`cycle` = `put` + `reserve` + `delete`（3 次协议往返）；`get` = `reserve` + `delete`；
`bury` 每轮把任务 bury 后再 kick（kick 按 tube 生效，每个客户端独立 tube）。

### HTTP `/health`（keep-alive）

| tier | conc | qps | p50 ms | p95 ms | p99 ms | served | stalled | wait p50 ms | errors |
|---|---|---|---|---|---|---|---|---|---|
| small | 1 | 65695 | 0.01 | 0.02 | 0.03 | 1 | 0 | 0.0 | 0 |
| small | 8 | 165450 | 0.05 | 0.07 | 0.08 | 8 | 0 | 0.1* | 0 |
| small | 16 | 164176 | 0.05 | 0.07 | 0.09* | 8 | 8 | 0.1* | 0 |
| small | 64 | 163388 | 0.05 | 0.07 | 0.09 | 8 | 56 | 0.1* | 0 |
| small | 128 | 161516 | 0.05 | 0.07 | 0.10 | 8 | 120 | 0.1 | 0 |
| small | 512 | 163689 | 0.05 | 0.07 | 0.09* | 8 | 504 | 0.1 | 0 |

### HTTP `/health`（每请求新建连接）

| tier | conc | qps | p50 ms | p95 ms | p99 ms | served | stalled | wait p50 ms | errors |
|---|---|---|---|---|---|---|---|---|---|
| small | 1 | 107* | 0.06* | 0.14* | 0.21* | 1 | 0 | 75.7* | 0 |
| small | 8 | 59* | 0.47* | 73.33* | 102.75* | 8* | 0 | 42.6* | 13419* |
| small | 16 | 309* | 0.57* | 2.79* | 995.67* | 16 | 0 | 1658.7* | 12082* |
| small | 64 | 5973* | 1.62* | 2.52* | 6.45* | 64 | 0 | 1090.8* | 7546* |
| small | 128 | 7* | 1150.86* | 1804.99* | 2147.28* | 35* | 1* | 5028.4* | 14969 |
| small | 512 | 199* | 131.43* | 4189.85* | 4858.13* | 225* | 106* | 5168.4* | 9645* |

**这组数字不能当作服务端容量**：`errors` 是客户端 `dial` 失败。独立复测
（`-conns per-request -conc 64 -warm 1000 -dur 5000`）得到 7686 个成功请求、
11577 次 `dial_err`、5 秒内 TIME_WAIT 峰值 32585，而可用临时端口只有 16384
（49152–65535，MSL 15 秒）。首次突发可达 1.76 万连接/秒，稳态被 TIME_WAIT 压到
约 1.1k 连接/秒。只有 `conc 1` 的首轮（约 1.7 万 rps）是有效读数。

### 长稳压测（soak）

低并发（`/health` conc 2 + narrow 查询 conc 1）跑 180 秒 + 采样，RSS 每 5 秒采一次。

| 启动 | 10 s 基线 | 结束 | 峰值 | 基线→结束 | 采样跨度 |
|---|---|---|---|---|---|
| 2.6 MiB | 18.9 MiB | 18.9 MiB | 18.9 MiB | +0.1% | 186 s |

| 指标 | 值 |
|---|---|
| RSS 采样次数 | 38 |
| 进程 FD 数（结束） | 16 |
| `oboe_http_requests_total` | 2 → 16665071（+16665069） |
| `oboe_queries_total` | 0 → 257933 |
| `oboe_uptime_seconds` | 0 → 185 |
| `oboe_rows` / `oboe_parts` / `oboe_partitions` | 200000 / 1 / 1（不变） |
| `oboe_disk_bytes` | 1558143（不变） |
| counter 单调性 | 全部单调不减 |

| label | conc | qps | p50 ms | p95 ms | p99 ms | errors |
|---|---|---|---|---|---|---|
| soak-health | 2 | 91150.80 | 0.02 | 0.05 | 0.07 | 0 |
| soak-query | 1 | 1433.00 | 0.62 | 1.15 | 1.46 | 0 |

结论：稳态内存不增长、无连接泄漏（FD 16、`oboe_http_connections_total` 只增加 4）、
counter 单调；185 秒共服务 1666 万次 HTTP 请求和 25.8 万次查询。

## 每档瓶颈归因

| 场景 | 主要瓶颈 | 证据 |
|---|---|---|
| `/health` keep-alive | 业务线程数（连接模型） | conc 8→512 全部 `served=8`、`stalled=conc−8`；吞吐 14.9–16.5 万封顶；单 worker 6.6 万 |
| `/health` 每请求新建连接 | 客户端源端口 / TIME_WAIT | 5 秒 1.15 万 `dial_err`；TIME_WAIT 3.26 万 > 16384 端口；首突发 1.76 万/s，稳态 ~1.1k/s |
| jsonline 单行 | HTTP 请求-响应往返 | 14–14.4 万 rows/s，p50 0.05 ms，与 `/health` 同量级 |
| jsonline/elastic/loki 批量 | 刷盘（zlib 压缩）与 memtable 积压 | 首轮 427/339/166 万 rows/s，第三轮 71/40/62 万；p99 0.8→11/55/32 ms；无错误 |
| filter（带 limit） | 命中前 500 条即停 | 8.1k 行扫描，20 万/500 万档都是 0.6 ms、约 1600 qps |
| stats | 单线程逐行扫描 + CPU | 125 万行 71.8 ms ≈ 17.4M rows/s；8 线程仅 4.7×（同机客户端争核） |
| sort | 全量扫描 + 排序 | 487 万行 279 ms；8 线程 14.2 qps（CPU 饱和），p99 703 ms |
| narrow | 扫描量 + 块粒度 | 时间裁剪按 8192 行块生效：small 扫 6.9 万/20 万，big 扫 90 万/500 万；8 线程 4.3× |
| 查询 conc 32 | 业务线程数 | `served=8`；stats 1185→1320 qps、sort 257→291，基本不再增长 |
| 缓存小值 | HTTP 层 | PUT/GET-hit/miss 均 ~15 万 ops/s，p50 50 µs，接近 `/health` 单连接上限 |
| 缓存 8 MiB | 内存拷贝带宽 | PUT 913 ops/s（7.3 GiB/s）、GET-hit 1309（10.5 GiB/s），p99 22–30 ms |
| 队列 | 全局锁 / 单 tube | 1→8 客户端约 2.1×，8→32 客户端仅 +5%；cycle p50 43 µs→637 µs |
| soak | 无 | RSS、FD、counter 均稳定 |

## 数据规模对比（20 万 vs 500 万）

| class | 20 万 qps（库内/1 线程） | 500 万 qps（库内/1 线程） | 倍数 | 扫描行数（20 万→500 万） |
|---|---|---|---|---|
| filter | 2161.20 | 2223.70 | 1.03× | 8192（不变，limit 截断） |
| stats | 254.80 | 11.60 | 0.046× | 5 万 → 125 万（25×） |
| sort | 55.80 | 3.00 | 0.054× | 20 万 → 487 万（24.3×） |
| narrow | 191.00 | 15.60 | 0.082× | 6.9 万 → 90.4 万（13.1×） |

查询吞吐与扫描行数近似成反比；`filter` 因 `limit 500` 在 8192 行截断，成本与数据量无关。
`stats` 在 500 万档有 459 个 block 被 bloom 剪枝；`sort`/`narrow` 的 `pruned_blocks`
为 0，但实际时间裁剪仍然生效（`scanned` 远小于总量）——**时间裁剪没有被计入
`pruned_blocks` 指标，是观测口径的缺口**，不是没有裁剪。

## 异常与已知问题

1. **写入时 memtable 无界增长**（最重要）：8 秒 `/insert/jsonline`（8 并发、
   100 行/请求）共 331005 个请求（3310 万行），摄入 41375 req/s，
   但 `oboe_mem_rows` 在 4 秒后就停在 **2314.85 万行**不再下降，RSS 峰值 **3.8 GiB**，
   进程 physical footprint 采样 7.6 GiB。`store_flush_partition` 的单线程排序 + zlib
   压缩吞吐约 120 万 rows/s，追不上 400 万 rows/s 的摄入；积压全部留在内存里。
   这是写入轮次间吞吐衰减的直接原因。
2. **SIGTERM 收尾时间过长**：上面同样的负载结束后，`store_close` 需要
   **33 秒**才把积压刷完（`sample` 显示主线程在 `part_writer_flush_block` →
   `zlib_compress`；监听已关闭但进程仍约 95% CPU）。矩阵写入阶段也触发了同样的
   超时（`shutdown.log` 记录了一次 8 秒未退出后 SIGKILL）。不是死锁，是慢关闭；
   但会拖长滚动升级/重启窗口。
3. **每请求新建连接的高并发不可测**：见上文 TIME_WAIT/端口耗尽；`conc≥8` 的
   qps/p99 中位数没有意义，只有 `conc 1` 首轮可引用。要测服务端连接建立能力，
   需要一台独立压测机或 `SO_REUSEPORT`/长连接混合负载。
4. **`pruned_blocks` 口径不完整**：只统计 bloom 剪枝，不统计时间范围剪枝，
   导致 narrow 查询看起来「没有裁剪」。建议在 `execute_query` 里把时间裁剪的
   block 也计入。
5. **同机压测**：服务端 + 压测端共享 10 个逻辑核，8 个业务线程再加 8–64 个客户端
   线程，CPU 竞争计入所有数字；吞吐结论偏向保守。loadgen 的 per-request
   `conc 64` 单测仍是同机，但已足以证明客户端端口瓶颈。
6. **`/metrics` 没有查询延迟/扫描行数的直方图**：只有计数；本套件通过库内
   `query` 模式补 `scanned/matched/pruned`，HTTP 路径看不到这些。

## 当前实现最大的性能短板

**写入/刷盘路径**，而不是查询或 HTTP 并发模型：

- 摄入 400 万 rows/s 与刷盘 ~120 万 rows/s 之间存在 3 倍缺口，memtable 只能无界增长，
  最终表现为 RSS 3–4 GiB、轮次间吞吐 6 倍衰减、退出 33 秒。
- 根因有三处：`store_ingest` 对整个 batch 持有 `s.mu` 写锁；刷盘只有
  `store_worker` 一个线程，压缩用单线程 zlib；`flush_max_rows=200k` 的切换点
  并不能阻止新数据在压缩期间继续堆积。
- 次一级短板是查询执行：每个查询单线程、逐行解码，`sort` 在 500 万行需要
  279–703 ms；并发只靠 8 个业务线程粗粒度并行，没有查询内并行或列式向量化。
- HTTP 的 worker-per-connection 模型是「已知设计取舍」：它让 keep-alive 并发
  严格等于 `-workers`，但把 512 并发稳定地变成 8 served + 504 stalled，
  行为可预测，不构成异常。

## 复现清单

1. `git worktree add /tmp/oboe_bench_ws bench-suite main`（或复用已有分支）。
2. `odin build bench -out:/tmp/oboe_bench -o:speed`、
   `odin build bench/loadgen -out:/tmp/oboe_loadgen -o:speed`。
3. `OUT=/tmp/oboe_matrix bench/run_matrix.sh`（完整）或加 `--quick`（冒烟）。
4. `$OUT/results/summary.md` 即本节表格；`raw.jsonl` 可自行重算；
   `shutdown.log` 记录被 SIGKILL 的收尾。
5. `odin test tests -all-packages` 应为 87 个测试全部通过。

## flush 阶段画像（步骤 0，2026-11-03）

目标：把 `store_flush_partition`（单 part 刷盘）和 `store_merge_partition`（多路合并）
拆成阶段计时，判断后续「P1 块级并行编码/压缩」和「P2 并行排序」值不值得做。
本轮只插桩，不改刷盘顺序、块大小、压缩参数。

### 结论

1. **sort 是刷盘第一大成本**：500 万行档占 40.7%（2.41 s / 5.91 s），是 zlib（16.8%）
   的 2.4 倍。排序比较器只比 `(stream, ts)` 两个整数，慢的原因是 5M 行（约 594 MiB）
   在内存里随机访问，属于带宽/缓存受限，而不是比较逻辑。
2. **压缩不是主成本，但仍是第二大单阶段**：zlib 占刷盘 16.8%（994 ms），合并里占 25.0%。
   加上「行→列 + bloom 哈希」36.5%，块内可并行部分合计约 58.5%。
3. **磁盘写入不是瓶颈**：data+bloom+index 的实际写入只占 0.6%（约 38 ms / 5M 行），
   且代码里没有 fsync，也没有 rename，`store_close` 的 33 秒全部花在 CPU
   （sort + encode + zlib），不是等盘。注意 `store_close` 还会 join 后台 worker
   （最长等一个 250 ms tick）：小档 wall 456.6 ms、drain 430.8 ms，而 flush 本身
   只有 205.6 ms，约 225 ms 是 worker 退出等待，不是刷盘。
4. **flush 侧几乎不等锁**：`store_flush_partition` 抢写锁（swap memtable）在 500 万行档
   为 0–132 ms（两轮之间不稳定，单轮标 `*`），swap 本身 7.5–10 µs/次。ingest 侧统计到的
   34%「等锁」是 8 个 ingest 线程互相排队（每批持锁约 55 µs，串行 append 上限
   ≈ 9.1M rows/s），不是刷盘抢占。
5. **块足够大，可以并行**：500 万行一次刷盘 616 块，608 块是满块（8192 行、
   平均 988 KiB 原始 / 46 KiB 压缩），原始块全部 ≤ 1 MiB。4 线程块级并行的粒度足够；
   但 20 万行只有 28 块（24 满块），小刷盘的并行收益有限。
6. **P1 值得做，但有上限**：块内阶段占 58.5%，4 线程理论上限
   `1/(0.415+0.585/4)=1.80x`，实际受 8 个 ingest 线程争 CPU 和内存带宽影响，预期 1.4–1.7x。
   **P2 也值得做，且单项收益更大**：数据只有 4 个 stream，按 stream 分段后各段
   `(ts)` 已独立，可 4 路并行排序，理论上限 `1/(0.593+0.407/4)=1.44x`，实现也简单。
   两步叠加（4 线程）理论上限约 3.9x；考虑带宽与实际可用核，落地预期 2–2.5x。
   若只能选一个，先做 P2（sort 单项更大、改动更小）；按既定计划先 P1 也能把
   merge 一起加速，随后 sort 会升到剩余时间的 ~69%，P2 的优先级反而更高。

### 测量方法

- worktree `/tmp/oboe_perf_ws`，基线 `32fed05`（`bench-suite` 当时已被另一会话推进到
  `afff796`，该提交含 P1 并行刷盘；为保持「不改生产行为」，本次测量固定在 `32fed05`）。
- 环境：Apple M4（10 逻辑核，macOS，APFS），Odin `dev-2026-10:84bc3fc21`；
  `odin build bench -o:speed`，无 `-microarch`；后台 IDE 使 load avg ≈ 9，
  绝对吞吐偏低，份额比绝对 ms 更可靠。
- 参数：8 ingest 线程、batch 500、block_rows 8192、worker 刷盘间隔 1 s、
  `flush_max_rows=200k`（与全套件一致），语料来自 `bench/corpus.odin`。
- 每档 3 次重复取中位数；表中 `*` 表示该行三轮 `(max-min)/median > 30%`。
  同一轮内的重复很稳；**整轮之间**的绝对耗时波动大：三轮 pipeline 分别为
  4.06 / 5.75 / 5.91 s（负载逐步升高），下表用最后一轮。
- 插桩：`logdb/profile.odin` 包级 profiler + 运行时开关，关闭时每个钩子只有一次
  relaxed atomic load，无分配。prof-off 对照（同样 3 次）显示开启相对关闭
  小档 −0.3%、大档 −0.2%～+2.7%（两轮），即插桩开销在噪声内。
- encode 内的 bloom token 哈希无法在不改变单遍缓存行为的前提下剥离（两遍法会让
  encode 膨胀约 20%），因此并入「encode:行→列+bloom哈希」；bloom 的定容/置位单列。

### 刷盘分阶段（单 part，中位数）

**20 万行**：ingest 23.5 ms（8.5M rows/s），flush 累计 205.6 ms，28 块，23.8 MiB 原始 → 1.5 MiB。

| 阶段 | 次数 | 总耗时 ms | 占比 | 每 block µs | rows/s | 压缩比 |
|---|---:|---:|---:|---:|---:|---:|
| sort | 1 | 75.54 | 36.9% | – | 2.65M | – |
| encode:行→列+bloom哈希 | 28 | 59.62 | 29.1% | 2129.4 | 3.35M | – |
| encode:serialize | 28 | 16.97 | 8.3% | 606.1 | 11.78M | – |
| zlib (level 1) | 28 | 38.58 | 18.8% | 1377.8 | 5.18M | 18.78x |
| bloom:finish | 28 | 12.50 | 6.1% | 446.5 | 16.0M | – |
| write data.bin | 28 | 0.96 | 0.5% | 34.2 | – | – |
| write bloom.bin | 28 | 0.38 | 0.2% | 13.6 | – | – |
| index + finish(index/meta/open) | 28 | 0.28 | 0.1% | – | – | – |
| **流水线合计** | | **204.8** | **100%** | | **0.98M** | |

**500 万行**：ingest 580.8 ms（8.6M rows/s），flush 累计 5933.0 ms（2 次），616 块，
594.3 MiB 原始 → 32.5 MiB。

| 阶段 | 次数 | 总耗时 ms | 占比 | 每 block µs | rows/s | 压缩比 |
|---|---:|---:|---:|---:|---:|---:|
| sort | 2 | 2407.69 | 40.7% | – | 2.08M | – |
| encode:行→列+bloom哈希 | 616 | 1708.28 | 28.9% | 2773.2 | 2.93M | – |
| encode:serialize | 616 | 450.13 | 7.6% | 730.7 | 11.1M | – |
| zlib (level 1) | 616 | 994.13 | 16.8% | 1613.8 | 5.03M | 21.46x |
| bloom:finish | 616 | 307.82 | 5.2% | 499.7 | 16.2M | – |
| write data.bin | 616 | 25.88 | 0.4% | 42.0 | – | – |
| write bloom.bin | 616 | 12.06 | 0.2% | 19.6 | – | – |
| index + finish(index/meta/open) | 616 | 2.46 | 0.0% | – | – | – |
| **流水线合计** | | **5908.5** | **100%** | | **0.85M** | |

锁与 memtable 切换（只列与刷盘有交互的部分）：

| 项 | 20 万行 | 500 万行 |
|---|---|---|
| ingest 等写锁（次数 / 每次 / 占流水线+锁） | 400 / 253 µs / 33.1% | 10000 / 304 µs / 34.0% |
| flush 等写锁（swap 前） | 0 / 0 µs | 0–132 ms（2 次，单次 0–66 ms，不稳定 `*`） |
| flush 追加 part 等锁 | <0.01 ms | <0.01 ms |
| swap 本身（持锁） | 10 µs | 7.5 µs / 次 |

块大小分布：20 万行 28 块（24 满 / 0 半 / 4 小块），平均 7143 行、869 KiB 原始 / 46 KiB 压缩；
500 万行 616 块（608 满 / 4 半 / 4 小块），平均 8117 行、988 KiB 原始（135–1001 KiB）/ 46 KiB 压缩，
原始块全部 ≤ 1 MiB。小块只出现在 stream 边界。

### 合并分阶段（强制 8 个 part → 1 个，中位数）

**500 万行**：merge 3945.9 ms（1.27M rows/s），输出 612 块；输入 640 块 / 594.3 MiB，读+inflate+decode 277.38 ms（2142 MiB/s）。

| 阶段 | 次数 | 总耗时 ms | 占 merge | 每 block µs | rows/s | 压缩比 |
|---|---:|---:|---:|---:|---:|---:|
| k-way 主循环 | 1 | 3934.01 | 99.7% | – | 1.27M | – |
| 输入读取+inflate+decode | 640 | 277.38 | 7.0% | 433.4 | 18.0M | – |
| encode:行→列+bloom哈希 | 612 | 1445.00 | 36.6% | 2361.1 | 3.47M | – |
| encode:serialize | 612 | 399.73 | 10.1% | 653.2 | 12.5M | – |
| zlib (level 1) | 612 | 985.37 | 25.0% | 1610.1 | 5.07M | 21.57x |
| bloom:finish | 612 | 310.57 | 7.9% | 507.5 | 15.9M | – |
| write data+bloom | 612 | 43.15 | 1.1% | 70.5 | – | – |
| **merge 总耗时** | 1 | **3945.90** | **100%** | | **1.27M** | |

**20 万行**：merge 162.4 ms（1.23M rows/s），输出 28 块；输入 32 块 / 23.8 MiB，读 11.45 ms（2076 MiB/s）；
encode 57.68 ms（35.5%）、serialize 15.88 ms（9.8%）、zlib 41.38 ms（25.5%）、bloom:finish 13.21 ms（8.1%）。

k-way 主循环里还有未单列的逐行开销：500 万行约 473 ms（12.0%）、20 万行约 35 ms（22.5%），
来自 8 路游标选路 + `part_writer_add`。后者在 merge 时 `copy_rows=true`，每行要把
7 个字符串复制进 block arena，5M 行约 3500 万次分配+拷贝。这是 merge 独有的成本，
P1/P2 都不会消除它。

### 其他发现

- `index:record` 只有 0.01 ms/块，`finish:index/meta/open` 合计 0.0%，索引与
  bloom 的「构建」都不是瓶颈；bloom 的大头是逐行哈希（含在 encode 内）。
- `finish:open+verify` 每次刷盘要重新打开 part、读回 index/bloom/meta，5M 档 1.8 ms/part，
  可忽略。
- 压缩比随档位变化：20 万行 18.78x，500 万行 21.46x，因为大档里重复值更多；
  P1 若改用更高压缩级别会同时抬高 zlib 占比，收益判断时要一起看。
- 本轮所有绝对吞吐低于文档前面记录的 1.0–2.1M rows/s，原因是后台 load avg ≈ 9；
  同机重复三轮的 pipeline 为 4.06/5.75/5.91 s，说明份额结论稳定、绝对值不要跨机对比。

## P2 并行排序（2026-11-03）

### 结论

- **sort 从 1810.8 ms 降到 411.4 ms（4.4x）**，500 万行；20 万行 83.0 → 28.4 ms（2.9x）。
  远超「至少 1.3x」的目标，不需要报告「不可行」。
- 收益一半来自**分区**、一半来自**线程**：先按 stream 分组让每个排序段变小（串行就快 1.37x，
  因为整数组 200 MiB 的随机访问被切成 4 段，缓存命中更好），再由 4 条线程并行（再快 3.2x）。
- 分区本身很便宜：5M 档「计数 + 稳定散列」41.7 ms，只占 sort 的 10%；回写还在段内排序之后。
- 默认配置下（P1 并行编码 + P2 并行排序）端到端刷盘 wall **3106 → 1584 ms（1.96x）**，
  整体 1.61M → 3.16M rows/s；sort 占 wall 从 58.3% 降到 26.0%。
- 200k 档 wall 只快 1.14x，因为 322 ms 里有约 250 ms 是等 250 ms tick 的 drain；看刷盘累计是
  116.0 → 71.3 ms（1.63x）。小档 < 100k 行退化为纯串行，不承担分区开销。
- merge 不经过本次改动（merge 用串行 `part_writer_add`），数字与上轮持平。

### 实现

新增 `logdb/sort_pool.odin`，`store_flush_partition` 里把 `sort_rows` 换成
`sort_rows_parallel(rows, s.cfg.sort_threads)`：

1. 顺序扫一遍统计每条 stream 的行数（stream 只有几条，线性扫描优于逐行 map 查找；
   超过 `SORT_MAX_STREAMS = 64` 时退回原整数组排序）；
2. 按 stream id 升序算前缀区间，再**稳定**地把行散列到各区间（同一条 stream 内保持输入顺序）；
3. 每个区间用同一套 in-place `sort_by(ts)` 排序，多个区间并发；最后按区间顺序回写。

确定性来自「分区与线程数无关、每段用同一算法排同一份数据」，所以 `sort_threads` 取 1 或 N
产出的 part 逐字节一致；不依赖稳定排序，也不引入 tie-break。段内比较器只比 `ts`，1 次比较
只剩 1 次整数比较（原比较器每次都要先比 stream）。线程数由 `Store_Config.sort_threads`
控制（`cmd -sort-threads`），`0` = `clamp(逻辑核/2, 2, 4)`；实际并发度再被区间数钳住，
所以 4 条 stream 最多 4 路。

`profile.odin` 的插桩保留，并补了两点：sort 拆成 `sort_scatter_ns` / `sort_segment_ns`
两个子阶段；P1 的并行编码 worker 各自持有 `Phase_Stats`，退出时加锁合并，因此
`flush-profile` 在默认配置下仍能给出逐 block 的 encode/zlib/bloom/写入拆分
（注意：此时阶段耗时是**多线程累计**，`流水线合计` 会大于 wall，wall 单独给出）。

### 前后对比（同一台机器、同一天、3 次中位数）

sort 单项（`flush-profile` 里唯一不受并行编码影响的计时）：

| tier | 改前（整数组 `sort_by`） | 改后 `-sort-threads 1`（分区串行） | 改后 `-sort-threads 4` | 相对改前 |
|---|---:|---:|---:|---:|
| 20 万行 | 83.0 ms | 67.4 ms | 28.4 ms | **2.9x** |
| 500 万行 | 1810.8 ms | 1328.6 ms | 411.4 ms | **4.4x** |

默认配置端到端（P1 并行编码 + P2 并行排序；改前为同分支 `57da6c4`，只含 P1）：

| tier | 刷盘累计 ms | wall ms | 整体 rows/s | sort 占 wall |
|---|---:|---:|---:|---:|
| 20 万行 改前 | 116.0 | 366.2 | 0.55M | 22.7% |
| 20 万行 改后 | 71.3 | 322.4 | 0.62M | 8.8% |
| 500 万行 改前 | 2491.2 | 3106.0 | 1.61M | 58.3% |
| 500 万行 改后 | 1082.8 | 1584.2 | 3.16M | 26.0% |

改后 500 万行分阶段（多线程累计 3634.9 ms；616 块，平均 8117 行 / 988 KiB 原始 / 46 KiB 压缩，
块分布与上轮一致，全部 ≤1 MiB）：

| 阶段 | 次数 | 累计 ms | 占累计 | 每 block µs |
|---|---:|---:|---:|---:|
| sort | 2 | 411.37 | 11.3% | – |
| └ 分组+散列 | 2 | 41.73 | 10% of sort | – |
| └ 段内排序 | 2 | 360.90 | 88% of sort | – |
| encode:行→列+bloom哈希 | 616 | 1557.27 | 42.8% | 2528.0 |
| encode:serialize | 616 | 454.49 | 12.5% | 737.8 |
| zlib (level 1) | 616 | 904.39 | 24.9% | 1468.2 |
| bloom:finish | 616 | 296.50 | 8.2% | 481.3 |
| write data+bloom | 616 | 7.56 | 0.2% | 12.3 |

### 确认确定性

新增 `logdb/sort_pool_test.odin`（5 个用例）：

- 4 条均分流各 25%：`sort_threads` 1 与 4 逐行（ts/stream/msg）一致；
- 一条 stream 占 90%、stream id 有空档（3..8 无数据）：一致；
- 只有 16 个不同 ts、每条 stream 大量重复：1 / 3 / 4 线程两两一致；
- 空表、单行、低于阈值的 1000 行：不崩、不乱；
- 120k 行分别用 1 与 4 线程排序后各写一个 part，`data.bin` / `bloom.bin` / `index.bin`
  逐字节相同。

排序只在同一条 stream、同一个 ts 的并列行之间改变了相对次序（旧实现对整个数组跑
pdqsort，并列次序本就取决于初始位置）；查询结果不受影响，跨线程数完全确定。

### 回归

- `odin build cmd` / `odin build bench` ✅；`odin test tests -all-packages` **94 全绿**
  （87 基线 + P1 的 2 个 + P2 的 5 个），`-define:ODIN_STRESS=1` 98 全绿。
- `bench -mode ingest`（8 线程，默认配置，3 次中位数）：20 万 7.83M → 7.14M rows/s、
  500 万 9.36M → 9.27M（−1%，在重复间波动内）；刷盘尾部这次本来就受后台 worker 时机影响，
  不用它做结论。
- `bench/run_matrix.sh --only small --reps 1 --no-soak`：播种 10.07M rows/s、刷盘 4.73M rows/s，
  所有 HTTP / 库内查询 / 缓存 / 队列格子 0 error。同一份数据、同样 8 线程跑两轮改前/改后二进制
  对照库内查询：filter 6.94k–9.73k vs 9.36k–9.51k qps、stats 1.21k–1.37k vs 1.34k–1.36k qps、
  sort 0.98k–0.99k vs 0.97k–1.00k qps（首轮是冷缓存，取区间看待），无退化。
- `flush-profile` 自对照（prof on vs off，默认配置）：20 万 +0.4%，500 万 −8.6%（落在噪声内，
  drain 里含 250 ms tick 等待）。

### 已知边界

- 并行度上限就是 stream 数：4 条 stream 最多 4 路，实测 500 万行 sort 4.4x 已经接近天花板；
  若某个 partition 只有 1–2 条 stream 占绝对多数，收益会回落到约 1.1–2x。
- 分区需要一块 `n * sizeof(Row)` 的临时数组（5M 行约 200 MiB），只在刷盘期间存在；memtable
  无界堆积到 2000 万行时峰值会多出约 800 MiB，若要收紧内存应改成按 stream 流式散列。
- 小于 100k 行不分区、不建线程，保持原整数组 `sort_by`。

环境与上一节相同：Apple M4（10 逻辑核）、macOS/APFS、`odin build bench -o:speed`、无
`-microarch`；所有对比都在同一天同一台机器上重测，避免跨轮的 load 差异。

## merge 零拷贝引用源块（2026-11-03）

### 结论

- **先量准**：merge 时 `copy_rows=true` 的逐行复制在本窗口内是 **1083.6 ms / 占 merge 10.7%**
  （500 万行），不是此前凭差额估的 473 ms；20 万行同样是 10.7%（43.2 ms）。差额估计偏低
  是因为复制成本的一部分被记在了 encode 的缓存代价里。行均 **14 次 arena 分配**
  （1 次 `msg` + 1 次 fields 数组 + 6 个字段 × 2 个字符串）、**329 B/行**，500 万行合计
  **7000 万次分配 / 1567 MiB 拷贝**，20 万行 280 万次 / 63 MiB。
- 因此 `copy_rows` 的**理论上限只有 1/(1-0.107) = 1.12x**，用户要求的 1.2x 在这个瓶颈结构下
  不可能达成。实测消除后 merge 快 **1.15x（20 万行）/ 1.18x（500 万行）**，略高于上限
  是因为少写 1567 MiB 也让紧随其后的 encode 变快（collect 4964 → 4796 ms）。
- **做法不是「批量分配 arena」，而是彻底不复制**：merge 输出块直接引用已解码的源块，
  源块改为「pin 到输出块 flush 之后再释放」。分配次数从 7000 万降到 0，拷贝字节数降到 0；
  同时删掉了 `Part_Writer` 里的 arena（刷盘路径本来就不用它）。
- merge 里真正的瓶颈在别处：**encode:行→列+bloom哈希 56.0%**、bloom:finish 15.0%、
  zlib 8.6%、serialize 7.1%、输入读 8.6%。所以下一轮该做 encode，不是 zlib。

### 实现

合并主循环原本把每一行的 `msg` 与 6 个字段字符串逐个复制进 part writer 的 arena，
目的只是让这些 `string` 在解码块被释放后仍然有效。改成：

1. `part_writer_add` 不再复制任何字符串（`Part_Writer.copy_rows` 与 `arena` 字段一并删除）；
   `part_writer_begin` 只保留 `prof_scope`，写盘路径 `write_part` / `write_part_parallel`
   本来就把整行数组活到 flush，契约不变。
2. `cursor_advance` 增加可选 `pins` 参数：块读完时不 `decoded_destroy`，而是把整个
   `Decoded_Block` 交给 merge 的 pin 列表；查询路径不传 `pins`，行为不变。
3. merge 在 `part_writer_add` 返回后比较 `pw.nblocks`：只有真的 flush 了输出块
   （`pw.rows` 已清空）才释放 pin。最后一个块由 `part_writer_finish` flush，pin 列表在
   proc 结尾统一释放。
4. pin 的同时存活上限是**游标数**（默认 8）：一个游标要消耗 8192 行才退役一块，而
   8192 行必然触发输出块 flush，所以两次 flush 之间每个游标最多退役一块。实测 500 万行
   累计 pin 640 块 / 594.3 MiB，同时存活 ≤ 8 块（约 8 MiB）。

不变的部分：块内仍是同一条 `encode_block`、同一个 zlib 参数、同样的块边界，
所以 `data.bin` / `bloom.bin` / `index.bin` 逐字节相同（见下）。

### 前后对比（同一台机器、同一天、3 次中位数）

merge 总耗时：

| tier | 改前 | 改后 | 加速 |
|---|---:|---:|---:|
| 20 万行 | 404.2 ms（0.495M rows/s） | 352.3 ms（0.568M rows/s） | **1.15x** |
| 500 万行 | 10083.0 ms（0.496M rows/s） | 8557.9 ms（0.584M rows/s） | **1.18x** |

500 万行分阶段（同一窗口，改前/改后）：

| 阶段 | 改前 ms | 占比 | 改后 ms | 占比 |
|---|---:|---:|---:|---:|
| 输入读取+inflate+decode | 738.2 | 7.3% | 735.2 | 8.6% |
| **逐行 copy(arena)** | **1083.6** | **10.7%** | **0（删除）** | – |
| encode:行→列+bloom哈希 | 4964.3 | 49.2% | 4796.5 | 56.0% |
| encode:serialize | 640.5 | 6.4% | 605.6 | 7.1% |
| zlib (level 1) | 746.3 | 7.4% | 740.2 | 8.6% |
| bloom:finish | 1287.2 | 12.8% | 1282.8 | 15.0% |
| write data+bloom | 176.1 | 1.7% | 11.5 | 0.1% |
| **merge 总耗时** | **10083.0** | **100%** | **8557.9** | **100%** |

20 万行：copy 43.2 ms（10.7%）→ 0；encode 197.6 → 191.2；serialize 25.9 → 24.3；
zlib 31.0 → 31.1；bloom:finish 51.5 → 51.9；merge 404.2 → 352.3。

（本窗口后台 load 比上一节高，绝对值整体放大约 2.5x；份额与同窗口前后比值才是结论依据。
`write data+bloom` 两轮差异属 I/O 抖动，改前行带 `*`。）

### 字节一致性与新增测试

新增 `logdb` 包内测试 `test_store_more_merge_zero_copy_part_bytes_match`：
3 批 × 40 行（3 条 stream、block_rows=32、merge_threshold=3）刷盘后 merge，
再把同一批行按 `(stream, ts)` 排好直接用 `write_part` 写一个参考 part，逐字节比对
`data.bin` / `bloom.bin` / `index.bin`。这保证「引用源块」和原来的「复制进 arena」
产出完全一致，part 格式没有被改动。

### 取舍

- 语义变化只在**内存生命周期**：解码块晚于「读完」释放，最多同时多留 8 个块（≈8 MiB）。
  行内容、块边界、索引、bloom 全部不变。
- 峰值内存互换：原来每次输出块要 bump 约 1 MiB 的 arena，现在改为持有 8 个已解码块；
  两者同量级，未观察到 RSS 上升。
- merge 不是多线程并发的（一条 partition 一个 merge），pin 列表不需要加锁。

### 回归

- `odin build cmd` / `odin build bench` ✅；`odin test tests -all-packages` **95 全绿**
  （94 基线 + 本轮的字节一致测试），`-define:ODIN_STRESS=1` 99 全绿。
- `bench/run_matrix.sh --only small --reps 1 --no-soak` 播种与查询无退化（见下）。
- `flush-profile` prof-off 自对照：20 万 −0.5%、500 万 −0.6%，插桩开销在噪声内。

## ingest 分片锁（2026-11-03）

### 结论

- 8 线程 ingest 的瓶颈确实是**同一把写锁把整批串起来**：改前每批持 `s.mu` 写锁，500 万行
  10000 次调用累计等锁 **6947.3 ms**，占流水线+锁 **38.8%**，每次调用 84% 的时间在排队。
- 把 memtable 按分区内分片（默认 10 = 逻辑核数）切成独立锁的追加缓冲后：ingest
  **1388.1 → 382.6 ms（3.6x）**，3.60M → **13.07M rows/s**；等锁合计占 **3.5%**
  （等 store 共享锁 0.2% + 等分片锁 3.3%），单批调用里等分片锁 14.5%，都低于 15%。
  20 万行同样 **51.3 → 15.7 ms（3.3x）**，等锁 32.7% → **2.6%**。
- 单线程不退化：3M 行 `-threads 1` 改前 2.965/3.004M rows/s、改后 2.990/2.949M，噪声内。
- 写锁粒度没有停在「分区级」：基准数据只有一个 UTC 日分区，分区级锁不会带来任何并发；
  真正起作用的是分区**内部**的分片。分片数不是线程数，而是 store 配置
  （`-ingest-shards`，0 = 逻辑核数，钳在 4..16），因此单线程与多线程看到的是同一套语义。

### 实现

1. `Partition` 的单个 `Mem_Part` 换成 `shards: []Ingest_Shard`，每个分片自带
   `sync.Mutex`、`[dynamic]Row`、独立 `common.Arena` 和 `bytes`。分片数在分区创建时固定，
   列表不再变化，所以读列表不需要额外锁。
2. `store_ingest` 整批只持有 **`s.mu` 共享锁**（防止 retention 在批次中途释放分区），
   然后按「连续同 UTC 日的行段」取一次**分片锁**：正常 500 行一批只加一次锁，
   不引入每行加锁。分片用 `next_shard` 原子轮转，所以即使 8 个线程都写同一个分区，
   也会落到不同分片。
3. 批次里出现尚未建分区的日（每天第一批）走慢路径：放掉共享锁 → 写锁建分区 → 重新取共享锁，
   不占用热路径。
4. flush 的 swap 仍在 `s.mu` 写锁下，但偷取内容时只拿各分片自己的锁：
   先取走该分片的 rows/arena 并把原子计数减掉，再重置分片。`s.mu` 写锁保证
   「查询正在扫 memtable」时不会看到行凭空消失（part 还没挂上）；分片锁保证
   正在追加到该分片的批不会漏行——追加在偷取之后发生的行留在新 memtable 里。
5. `flush_max_rows` / `flush_max_bytes` 触发条件不变，改为读分区上的原子累计值
   （分片内追加时加、偷取时减，加减都在该分片锁内完成，因此不会出现负数）。
   `store_stats.mem_rows` 同样是这些原子值之和。
6. `flush_interval` 用分区级 `since_ns`：变空时清零，下一次追加时写入当前时间。
   极端情况下（清零与并发追加刚好交错）新 memtable 的 `since_ns` 可能为 0，导致
   下一个 tick 提前刷一次（不丢行、只是多一个小 part），这是有意的取舍。
7. 查询路径 `execute.odin` 扫 memtable 时改为逐分片持锁扫描，避免与并发追加竞争
   `[dynamic]Row` 的扩容。

### 前后对比（同一台机器、3 次中位数）

| tier | 改前 ingest | 改后 ingest | 加速 | 等锁占比（改前 → 改后） |
|---|---:|---:|---:|---:|
| 20 万行 | 51.3 ms（3.90M rows/s） | 15.7 ms（12.78M rows/s） | **3.3x** | 32.7% → **2.6%** |
| 500 万行 | 1388.1 ms（3.60M rows/s） | 382.6 ms（13.07M rows/s） | **3.6x** | 38.8% → **3.5%** |

500 万行锁明细（`flush-profile`，8 线程，10000 次 ingest 调用）：

| 项 | 改前 | 改后 |
|---|---:|---:|
| ingest 等写锁 / 等 store 共享锁 | 6947.3 ms（694.7 µs/次） | 21.4 ms（2.1 µs/次） |
| ingest 等分片锁 | – | 380.4 ms（38.0 µs/次） |
| ingest 持锁累计 | 1327.1 ms | 2605.0 ms（现在是真的并行干活） |
| flush 等写锁 | 0.19 ms | 0.19 ms |
| memtable swap 持锁 | 0.02 ms | 0.17 ms（10 个分片各拿一次锁） |
| 流水线合计 / 锁等待占比 | 10987 / 41.0%* | 11046 / **3.5%** |

\* 改前那次 flush-profile 的 lock 等待里 2.6% 是「flush 追加 part 等锁」，本窗口为 0；
两个窗口的后台 load 不同，绝对值不可直接比，**占比**才是结论。

改后 ingest 每批的持锁时间从 ~133 µs 涨到 ~260 µs，是因为改前 8 个线程 84% 的时间在排队、
真正干活的时间被稀释；现在 8 个线程同时拷贝，累计 thread-time 更接近 wall×8。

### 语义与顺序取舍

- **不变**：`flush_max_rows` / `flush_max_bytes` 触发阈值、retention、memtable 行数指标、
  swap 与 ingest 的一致性（不丢行、不重复）。`ingest_bytes` 统计口径不变。
- **变了（1）**：memtable 内的行顺序。改前是全局到达顺序；改后是「按分片轮转追加，
  flush 时按分片顺序拼接」。排序后 part 仍按 `(stream, ts)` 排列，但 `(stream, ts)`
  **完全相同**的行之间的相对顺序可能不同。既有逐字节断言都不受影响：
  `sort_pool_test` 直接构造数组做 1 vs 4 线程对比、`test_store_more_merge_zero_copy_part_bytes_match`
  用的是唯一 `ts`，都不经过分片追加；`test_store_ingest_shards_concurrent_flush` 也只用唯一 `ts`。
  并发 ingest 时到达顺序本来就是非确定的，所以这个取舍只在单线程写入时有可见差别。
- **变了（2）**：查询不再把 ingest 挡在门外。改前 ingest 持写锁，查询持共享锁，二者互斥；
  改后查询与 ingest 都是共享锁 + 分片锁，查询可能看到扫描过程中新到的行（也可能看不到
  正在别的分片追加的行）。这是并发追加的必然结果，换来的是 ingest 不再被查询的长扫描阻塞。
- 每个分区有 `ingest_shards` 个 arena，每个 1 MiB chunk 惰性分配：活跃分区常驻约
  `ingest_shards` MiB，flush 后归还。分片数上限 16 就是为了封住这部分内存。

### 新增测试

`logdb/shard_test.odin: test_store_ingest_shards_concurrent_flush`（普通与 STRESS 都会跑）：
6 个写线程 × 50 批 × 40 行 = 12000 行，跨 2 个 UTC 日，期间另一个线程循环
`store_flush_due` + `store_flush_all`。结束后断言：

- `store_stats().rows == 12000`、`mem_rows == 0`、`parts > 0`；
- 逐块 `read_block` 扫描全部 part，每个 `ts` **恰好出现一次**（无丢行、无重复、无崩溃）。

### 回归

- `odin build cmd` / `odin build bench` ✅；`odin test tests -all-packages` **96 全绿**
  （95 + 本轮的并发测试），`-define:ODIN_STRESS=1` **100 全绿**；普通套件连跑 5 次全绿。
- `bench/run_matrix.sh --only small --reps 1 --no-soak`：播种 12.6M rows/s（改前 10.07M）、
  查询无退化（见下）。


## encode 一遍扫描：列名缓存 + bloom 值去重（2026-11-03）

### 结论

`encode collect`（行 → 列数组 + bloom token 哈希）从 merge 的 45.4% 降到 30.8%，
**5M 档 collect 1102.3 ms → 547.4 ms（2.01x）**，merge 总耗时 **2429.0 ms → 1777.5 ms（1.37x）**；
顺带 `bloom:finish` 也快了 **3.05x**（146.1 → 47.9 ms），因为去重后的哈希表小了 6 倍。
part 字节完全未变（golden SHA-256 测试守门）。目标（collect ≥1.3x）达成。

### 先量准：collect 里到底是谁贵

把融合的行循环临时拆成「只列化」「只 bloom」两趟，在合并路径上测得的份额是
**列化 14% / bloom 86%**（5M：157.1 ms / 928.1 ms）。所以这一轮的收益必须来自 bloom，
单纯"把两次字段遍历合成一次"最多只能省列化那一小半。

拆成两趟的副作用也一并量了：合并路径几乎同价（1085 vs 1102 ms），但 flush 路径从
1532.9 ms 涨到 2130.6 ms —— 两趟意味着两遍 full scan，在 8 个并行 encode worker 争内存带宽时
掉得更狠。所以**保留融合的单趟循环**，只在里面把重复工作去掉。

### 贵在哪：每个值都在重算，重复值也在重算

改前每个 (field, value) 都要：

1. `find_enc_col` 线性扫列表找列（7 列 × 7 次查找/行）；
2. `bloom_builder_add_value` 里用 `fnv(field)` 重算字段名前缀；
3. 为了命中 memo，先对**整个 value 做一遍 FNV**（msg ≈ 65 B），再拿 `full_h` 查 256 槽直接映射表；
4. 命中 memo 还要把 hash 列表 `copy` 一遍追加进 `bb.hashes`。

第 3 步是关键浪费：语料里 msg 只有 32 种、level 4 种、service 4 种、zone 3 种、duration 256 种，
但每一行都把 65 B 的 msg 重哈希一次只为确认"又是它"。

### 实现

1. **列名字典**：每个 block 一个 32 槽开放寻址表（键 = 长度 + 尾部 8 字节的 FNV），列解析从
   线性扫描变成 O(1) 平均；`_msg` 固定为第 0 列，热循环里直接写，不做查找。
2. **字段前缀只算一次**：`Enc_Col.base = fnv(name) + "\0"`，新增
   `bloom_builder_add_value_base(bb, base, value)`；每列每 block 一次，而不是每值一次。
3. **每个列一份 64 槽值缓存**（`keys / cached / used`）：键 = 长度 + 尾部 8 字节 FNV，
   命中要求**整串相等**（`cached[slot] == value`），键碰撞只会退化成走完整路径。
4. **重复值直接跳过 bloom**：块的 bloom 只保留哈希**集合**——`bloom_builder_finish` 本来就用
   开放寻址表把重复哈希折叠成 `uniq` 个，再由 `uniq` 决定 `nbits`，置位是 OR。所以同一个
   (列, 值) 在块内第二次出现时**不追加任何哈希**，输出位图逐位相同。这是
   "`n` 变小但 `uniq` 不变 → `target` 不变 → `nbits` 不变"的等价变换，不是近似。
5. 列化与 bloom 在**同一次字段遍历**里完成，一个字段只解析一次名字。

缓存全部是 block 内局部的（`Enc_Schema` 在 `encode_block` 栈上，块结束即销毁），
不跨块、不持有字符串指针，merge 的 pin 生命周期不受影响。

### 同窗口前后 A/B（改前/改后二进制交替各跑 2 轮，每轮 3 reps 取中位数）

merge 500 万行：

| 项 | 改前 | 改后 | 变化 |
|---|---:|---:|---:|
| merge 总耗时 | 2429.0 / 2446.3 ms | 1777.5 / 1783.8 ms | **1.37x** |
| encode:行→列+bloom哈希 | 1102.3 / 1111.7 ms（45.4%） | 547.4 / 548.0 ms（30.8%） | **2.01x** |
| bloom:finish | 146.1 / 146.4 ms | 47.7 / 48.1 ms | **3.05x** |
| encode:serialize | 191.9 / 192.0 ms | 191.3 ms | 持平 |
| zlib (level 1) | 737.4 / 741.1 ms | 741.1 / 737.4 ms | 持平 |

merge 20 万行：总耗时 100.35 / 101.79 → 74.5 / 75.5 ms（**1.35x**）；
collect 43.4 / 44.3 → 21.6 / 21.8 ms（**2.03x**）；bloom:finish 5.97 / 6.09 → 2.09 / 2.14 ms。

单 part 刷盘（flush 500 万行，`flush_threads` 自动 = 并行 encode）：

| 项 | 改前 | 改后 | 变化 |
|---|---:|---:|---:|
| 流水线合计 | 3544.1 / 3537.7 ms | 2811.6 / 2803.7 ms | **1.26x** |
| encode collect | 1532.9 / 1540.5 ms | 1096.0 / 1093.3 ms | **1.40x** |
| bloom:finish | 279.1 / 276.3 ms | 90.8 / 89.7 ms | **3.08x** |

刷盘档的 collect 只快 1.40x（合并档 2.01x）：flush 用多个 encode worker 并行跑，
`collect_ns` 是 thread-time 累计，8~10 个 worker 同时做哈希时内存带宽先到顶，
省下来的哈希量换不成等比例的 thread-time。合并路径是单线程编码，收益接近理论上限。

口径提醒：本窗口 load 比上一轮低（基线 merge 2429 ms，上一轮同档 8558 ms），
所以这里只做**同窗口交替 A/B**，不跨窗口比绝对值。

### golden 测试：字节不变由常量守门

`logdb/part_golden_test.odin: test_part_golden_bytes_sha256`

- 固定语料：3 个 stream × 40 行、`block_rows = 16`、5 个字段（每 stream 一个常量列 `zone`、
  缓慢变化的 `level`、每行变化的 `service/host/duration`）+ `_msg`，时间戳固定。
- 用 `write_part` 落盘后对 `data.bin` / `bloom.bin` / `index.bin` 取 SHA-256，
  与常量逐字符比较，并额外断言三个文件的确切字节数：

| 文件 | SHA-256 | 字节 |
|---|---|---:|
| data.bin | `7927ae0d92f889d4cade54efe411304fe0eab1868869ac92297959c0a3856f2d` | 3812 |
| bloom.bin | `56d0e2b17f3e622f1ea85d419e986f3803ce59a6b485a0f49eaf957d36e93e53` | 1920 |
| index.bin | `1f0413538ef30352d3c6a80ee70a9ab11fb022d6fa1c1ee3327e9e4f52a95abc` | 588 |

（`meta.json` 含 `created_ns`，不参与指纹。）常量由**优化前的编码器**生成，之后任何人动 encode
都会被这条测试拦下。要改常量就等于改格式，必须同时改 `BLOCK_VERSION` 并在本文档留记录。

保留了原有的字节断言：串行 vs 并行排序一致性、`test_store_more_merge_zero_copy_part_bytes_match`
（merge 输出与参考 part 逐字节比对）。

### 回归

- `odin build cmd` / `odin build bench` ✅；
  `odin test tests -all-packages` **97 全绿**（96 + 本轮 golden 测试），
  `-define:ODIN_STRESS=1` **101 全绿**。
- `bench/run_matrix.sh --only small --reps 1 --no-soak`：EXIT=0、`failures.txt` 空，
  播种 36.2M rows/s、flush 5.52M rows/s，查询 filter 8784 qps @8 / stats 1737 qps @8 / sort 350 qps @8，
  无退化。

## 有界背压：memtable 硬上限、有界等待与 503（2026-11-03）

### 结论

memtable 从"只有触发阈值、没有上限"改成**硬上限 + 阻塞等待 + 有界等待**：

- 新参数 `-mem-max-rows`（默认 600 万）与 `-mem-max-bytes`（默认 1 GiB）：整库（所有分区
  合计）的 memtable 估算量超过上限时，`store_ingest` 先在条件变量上等 flush 释放空间，
  而不是继续堆积。
- 等待有界：`-mem-wait`（默认 2 s）。等满预算仍放不下就**拒绝整批**，HTTP 三个写入端点
  （`/insert/jsonline`、`/insert/elasticsearch/_bulk`、`/insert/loki/api/v1/push`）返回
  `503` + `Retry-After: 1` + `Connection: close`，不再把请求挂死。
- 实测（8 s、8 并发、100 行/请求，jsonline）：默认 cap 下 **RSS 峰值 3171 MB**、
  **SIGTERM 退出 1942 ms**、`store_close` 1852.9 ms；cap 关闭时 8 s 内 RSS 就**越过
  4 GiB**（第 5.9 s 触发监控保护被强杀），证明上限确实把"无界"变成了"有界"。
- 吞吐：这条 HTTP 写入路径本身的上限约 1.0–1.2M rows/s，cap 开/关都在同一水平
  （1.21M vs 1.20M），**背压在此路径上没有吞吐代价**；把等待预算调到 100 ms 时
  5 s 内出现 168 次 503，请求最大延迟 103 ms。
- 与本轮无关的 flush/merge 路径完全没变：同窗口交替 A/B 全部落在 ±1% 内。

### 为什么默认是 6M 行 / 1 GiB

按 jsonline 语料实测约 **330 B/行**（消息 + 字段名/值 + `Field` 结构 + 分片 arena 头）：

```
6_000_000 行 × ~330 B/行 ≈ 1980 MB ≈ 1.84 GiB
1 GiB = 1024 MiB（字节上限，实际先触发：约 3.26M 行耗尽）
```

两个上限同时生效、取更严的那个；默认配置下**字节上限先到**（`-mem-max-rows` 是兜底，
防止行很小的负载把字节估算摊薄）。`flush_max_rows = 20 万`，所以上限是它的 30 倍——
flush 跟得上时永远不会触发等待，跟不上时最多驻留 ~1 GiB。

**RSS 峰值约为上限的 3 倍**，因为 flush 的工作集与 memtable 重叠：上限一刻的 arena
（≈1 GiB）+ 偷出的行副本（`old_rows`）+ 排序/编码缓冲。实测 1 GiB cap → 3171 MB 峰值、
512 MiB cap（`-mem-max-rows 3000000 -mem-max-bytes 512MB`）→ 1670 MB 峰值。
小内存机器建议直接从 512 MiB 起。

### 实现

1. **整库记账**：`Store` 增加 `mem_rows` / `mem_bytes` 原子计数，ingest 在追加前按
   `row_bytes` 预扣，flush 偷走 memtable 后归还，retention 删分区时也归还。
2. **上限检查与预扣在同一临界区**（`space_mu`）：避免"检查通过 → flush 扣减 → 两个批次
   各自超卖一次"的竞态。
3. **先预扣、再拿 store 锁**：`reserve_mem` 在 `s.mu` **之前**完成，等待时只持有
   `space_mu`；flush 的顺序是 `mu -> space_mu`。因此锁依赖是单向的（`mu → space_mu`），
   不存在 ingest 等 flush、flush 等 ingest 的环。
4. **有界等待**：`sync.cond_wait_with_timeout`，每次醒来重算剩余预算并重新检查；等满
   `mem_wait` 返回 `rejected`。`store_ingest_bounded` 返回
   `(accepted, stalled_ns, rejected)`，旧的 `store_ingest` 保留为薄包装。
5. **压力触发提前 flush**：memtable 用量 ≥ 上限一半时，后台线程把"有行的分区"全部落盘，
   不必等 `flush_interval` 或 `flush_max_rows`，让被阻塞的写入在下一次 tick 拿到空间。
6. **关停唤醒**：`store_close` 先置 `stop` 再 `cond_broadcast`，等待者在超时前立即
   返回 `false`（不会睡满 2 s）；同时 **merge 在 block 边界检查 `stop` 并中止**——
   否则一次大合并会把 SIGTERM 拖成 10 s+（实测 20 s 压测时曾出现 11.7 s）。
   中止的合并不写 `merged_from`，源 part 原样保留，下次 tick 重做。
7. **HTTP 语义**：三个写入 handler 都走 `store_ingest_bounded(srv.store, rows, mem_wait)`，
   `503` 统一补 `Retry-After: 1` 并 `Connection: close`（`mark_ingest_overload`）。

### 同窗口前后 A/B（本轮提交 vs 上一轮提交，二进制交替各跑 2 轮 × 3 reps）

本轮只动写入准入与关停，不碰 encode/flush/merge 的字节与算法，所以 `flush-profile`
应当**没有变化**——实测也确实没有：

| 项 | 上一轮（c7b62c8） | 本轮 | 变化 |
|---|---:|---:|---:|
| flush 500 万 wall | 3165.3 / 3201.0 ms | 3199.1 / 3203.8 ms | +1.1% / +0.1% |
| merge 500 万 总耗时 | 6251.6 / 6232.0 ms | 6214.7 / 6214.8 ms | −0.6% / −0.3% |
| flush 20 万 wall | 379.0 / 378.0 ms | 377.5 / 378.2 ms | −0.4% / +0.1% |
| merge 20 万 总耗时 | 253.1 / 253.0 ms | 253.5 / 252.7 ms | +0.2% / −0.1% |
| encode collect（flush 500 万） | 4304.2 / 4445.6 ms | 4393.9 / 4377.0 ms | +2.1% / −1.5% |

（本轮窗口的绝对值又比上一轮高 ~2.4x，仍是机器 load 所致，只看同窗口交替比。）

### 背压实测（`bench/loadgen` 打 `/insert/jsonline`，8 s、8 并发、100 行/请求）

| 配置 | RSS 峰值 | SIGTERM 退出 | `store_close` | 摄入 rows/s | 累计 stall | 503 |
|---|---:|---:|---:|---:|---:|---:|
| 默认（6M 行 / 1 GiB，`mem-wait` 2 s） | 3171 MB | 1942 ms | 1852.9 ms | 1.21M | 23.6 s | 0 |
| 512 MiB（3M 行 / 512 MiB） | 1670 MB | 1120 ms | 987.0 ms | 1.03M | 30.4 s | 0 |
| 512 MiB + `-mem-wait 100ms`（5 s） | — | 1127 ms | 1068.4 ms | 1.06M | 18.2 s | 168 |
| cap 关闭（`-mem-max-rows 0 -mem-max-bytes 0`） | **>4096 MB @5.9 s（被强杀）** | — | — | 1.20M | — | — |

- **RSS 有界**：默认 cap 8 s 峰值 3171 MB；同样配置跑到 20 s（23.3M 行）峰值
  1761/1779 MB（512 MiB cap），**不随写入时长增长**。
- **`store_close`**：默认 cap 1.85 s（其中 final flush 21 ms，其余是等后台线程把
  手上的 part 写完）；512 MiB cap 0.99 s。“最多等一个进行中的 flush + 最后一次
  flush”是设计上的上界。
- **SIGTERM**：1.94 s / 1.12 s / 1.13 s，全部 ≤ 2 s；`stop` 置位后 merge 在 block
  边界中止（`join` 耗时 253 ms）。
- **吞吐**：HTTP 写入路径自身约 1.0–1.2M rows/s，cap 开/关没有差别。也就是说在这条
  路径上"背压换稳定"没有额外代价；真正的取舍出现在库内批量 ingest（13M rows/s 级别），
  那时摄入会被压到 flush 速率附近——这是有意为之。
- **503 语义**：预算 100 ms 时 5 s 内 168 个批次被拒，`loadgen` 的 `max_ms` 只有
  103 ms（不会挂住）。响应示例：

```
HTTP/1.1 503 Service Unavailable
Content-Type: application/json
Connection: close
Retry-After: 1

{"error":"memtable full","accepted":0,"rows":100,"errors":0}
```

### 新增指标（`/metrics`）

| 指标 | 含义 |
| --- | --- |
| `oboe_mem_rows` / `oboe_mem_bytes` | 当前 memtable 的整库用量（`mem_rows` 复用已有指标） |
| `oboe_mem_limit_rows` / `oboe_mem_limit_bytes` | 当前生效的上限（0 = 不限） |
| `oboe_ingest_stalled_seconds_total` | 所有 ingest 批次在 cap 上累计等待的秒数（多线程求和） |
| `oboe_mem_limit_rejected_total` | 等满 `mem_wait` 仍放不下而被拒绝（→503）的批次数 |

`oboe_ingest_stalled_seconds_total` 是**线程累计时间**：8 并发各等 1 s 就是 +8。

### 测试

- `logdb.test_store_mem_cap_concurrent_ingest_flush`：cap 200 行、6 写线程 × 30 批 ×
  40 行（7200 行）+ 同时 flush。断言行数守恒（`ingested_rows == 7200`、逐块回读
  7200 行、无重复）、峰值 `mem_rows <= 200`、`stalled_ns > 0`、flush 后 memtable 清空、
  `store_close < 2 s`。
- `logdb.test_store_mem_cap_rejects_after_bounded_wait`：cap 100 行、无 flusher、
  预算 200 ms。断言第二批被拒、等待落在 `[200ms, 700ms)`、`mem_limit_rejected == 1`、
  已写入的 100 行仍在。
- `logdb.test_store_mem_cap_shutdown_wakes_waiters`：填满后起一个 `mem_wait = 5s` 的
  阻塞写线程，100 ms 后置 `stop` + 唤醒，断言写线程在 2 s 内退出。
- `logdb.test_store_merge_aborts_on_shutdown`：`stop` 已置位时 `store_merge_partition`
  必须不开始、part 数与行数不变、目录里没有半成品。
- `web.test_dispatch_ingest_overload_503`：cap 1 行、`mem_wait 0`，三个写入端点都返回
  503 + `Retry-After: 1`，行数仍为 0。

### 回归

- `odin build cmd` / `odin build bench` ✅；
  `odin test tests -all-packages` **102 全绿**（96 基线 + golden + 本轮 5 个），
  `-define:ODIN_STRESS=1` **106 全绿**。
- `bench/run_matrix.sh --only small --reps 1 --no-soak`：EXIT=0、`failures.txt` 空、
  无 `shutdown.log`；播种 36.2M rows/s、flush 5.52M rows/s，查询无退化。
