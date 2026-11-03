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
```

| 参数 | 含义 |
|---|---|
| `-mode ingest\|query\|cache\|queue-tcp\|all` | 子命令；`ingest` 默认写 `-data` |
| `-rows` / `-threads` / `-batch` | 播种行数、并发线程、每批行数 |
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
