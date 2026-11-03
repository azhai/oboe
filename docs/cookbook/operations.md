# 运维

本文面向部署和日常维护：数据目录、保留策略、合并、容量估算、备份恢复、安全边界
和已知限制。所有行为都对应当前实现；参数名以 `cmd/main.odin` 的 flag 为准。

## 1. 数据目录与 part 布局

默认数据目录是 `./data`（`-data` 可改）。目录按 UTC 天分区，每个分区下是若干不可变
part：

```text
data/
  partitions/
    20261026/                   # UTC 天，8 位数字
      parts/
        p_0000000000000007/     # p_ + 16 位十六进制序号
          meta.json
          index.bin
          data.bin
          bloom.bin
          merged_from           # 仅合并产物有
```

- `meta.json` 是 `Part_Meta`：`version`、`id`（等于目录名）、`rows`、`blocks`、
  `min_ts`、`max_ts`、`data_size`、`bloom_size`、`index_size`、`created_ns`。
- `index.bin`：12 字节头（魔数 `ODLI`、版本、记录大小、块数）+ 每块 64 字节记录。
- `data.bin`：每块一个独立 zlib 流，块内按列编码；一个块只包含同一个 stream 的行，
  行数不超过 `-block-rows`（默认 8192）。
- `bloom.bin`：每块一份 bloom filter，按唯一 token 数约 10 bit/项向上取 2 的幂，
  最小 1024 bit（128 字节），最大 32 KiB。
- `merged_from`：合并产物写入，每行一个被替换 part 的 id。启动时读取所有 marker，
  剔除已被取代的旧 part，避免崩溃后重复计数或复活旧数据。

检查命令：

```bash
DATA=/var/lib/oboe

# 有哪些天分区
find "$DATA/partitions" -mindepth 1 -maxdepth 1 -type d | sort

# 每个分区有哪些 part
find "$DATA/partitions" -mindepth 3 -maxdepth 3 -type d | sort

# 查看 part 元数据
for f in "$DATA"/partitions/*/parts/*/meta.json; do
  echo "== $f"
  cat "$f"
done

# 磁盘占用（含 meta.json、目录和文件系统开销）
du -sh "$DATA"
du -sh "$DATA"/partitions/*
```

验证：`find` 输出的天目录应为 8 位数字，part 目录应为 `p_` 加 16 位十六进制；
`meta.json` 中 `blocks` 应与 `index.bin` 的块数一致，`data_size` / `bloom_size` /
`index_size` 应与对应文件大小一致。

注意：

- `store_stats.disk_bytes`（以及 `/metrics` 的 `oboe_disk_bytes`）只累加
  `data.bin` + `bloom.bin` + `index.bin`，**不含** `meta.json`、目录项和文件系统
  块开销，所以 `du` 会略大。
- part 只有在 `meta.json` 写完后才算完整。启动时 `open_part` 会校验 meta、index、
  bloom、data 的大小和范围；校验失败的 part 目录会被直接删除，不会有隔离区。
- 同一个数据目录不能被两个进程同时打开；实现没有文件锁。迁移/恢复前先停旧进程。

## 2. 写入、flush 与持久化窗口

`store_ingest` 只写内存 memtable。后台线程每 250 ms 检查一次，满足以下任一条件就把
该分区落盘：

- 行数达到 `flush_max_rows`（默认 20 万）；
- memtable 估算字节达到 `flush_max_bytes`（默认 128 MiB，**按分区**计算）；
- 最旧一行等待超过 `flush_interval`（默认 1 s）。

因此：

- 进程被 `kill -9`、OOM 杀死或崩溃时，未 flush 的 memtable 会丢失。默认
  `flush_interval = 1s`，最坏大约丢最近 1 秒的数据；写入量很大时可能更早触发行数/
  字节阈值，实际窗口更小。
- 正常调用 `store_close` 会先 `store_flush_all` 再关闭 part；库程序应走这条路径。
  `cmd` 已安装 SIGINT/SIGTERM 处理器：第一次信号只置停标志并关闭监听，让
  `server_run` 返回，随后主线程的 `defer` 链执行 `server_stop`、`server_destroy`、
  `cache_save_file`、`queue_destroy` 和 `store_close`（其中包含 flush）；第二次
  信号直接 `_exit(128+sig)`，不再走清理路径。
- 需要让数据立即落盘时，库程序调用 `store_flush_all(s)`；HTTP API 没有 flush 端点。

检查 memtable 和磁盘可见行数：

```bash
curl -sS 'http://127.0.0.1:9428/metrics' \
  | grep -E '^oboe_(mem_rows|rows|parts|partitions|disk_bytes) '
```

验证：写入后 `oboe_mem_rows` 先增加；等待超过 `flush_interval` 后应下降，`oboe_rows`
和 `oboe_parts` 增加，`oboe_disk_bytes` 增加。`oboe_rows` 只统计已落盘 part，不包含
memtable；查询会同时扫描两者。

### 2.1 背压：memtable 上限与 503

memtable 有**硬上限**：`-mem-max-rows`（默认 600 万行）与 `-mem-max-bytes`（默认
1 GiB），按整库（所有分区合计）计算，任一到顶就**阻塞写入**，等后台 flush 释放空间，
而不是继续吃内存。这两个上限都可以设 0 表示不限（仅用于压测对照，不推荐生产）。

**写入可能被 503。** 阻塞不是无限的：`store_ingest` 最多等 `-mem-wait`（默认 2 s）。
等满预算仍放不下时整批被拒，三个写入端点返回：

```
HTTP/1.1 503 Service Unavailable
Content-Type: application/json
Connection: close
Retry-After: 1

{"error":"memtable full","accepted":0,"rows":100,"errors":0}
```

| 端点 | 503 body |
| --- | --- |
| `/insert/jsonline` | `{"error":"memtable full","accepted":0,"rows":N,"errors":M}` |
| `/insert/elasticsearch/_bulk` | `{"took":T,"errors":true,"items":[]}` |
| `/insert/loki/api/v1/push` | `{"error":"memtable full","retry_after":1}` |

约定：

- `503` 表示**这一批完全没写入**（`accepted = 0`），客户端应整批重试，不要只重发一部分；
  建议按 `Retry-After`（1 s）退避，别立即重锤。
- 响应带宽 `Connection: close`，连接会被关闭；用连接池的客户端需要重建连接。
- 只有"等满预算"才会 503。预算内等到了空间就正常返回 2xx，此时请求延迟会升高
  （最长约 `-mem-wait`），这是有意用延迟换 RSS 有界。

**调优**：

- 触发条件要满足"写入持续快于 flush"。短促的高峰一般等不到 2 s 就会拿到空间。
- RSS 峰值约为上限的 **3 倍**（flush 期间的 memtable arena + 偷出的行副本 + 排序/编码
  缓冲会同时存在）。1 GiB 上限 → 实测 3171 MB；512 MiB 上限 → 1670 MB。小内存机器从
  `-mem-max-bytes 512MB` 起，并给 `-mem-wait` 留 1–2 s。
- 想更早暴露压力而不是被动等满预算：把 `-mem-wait` 调小（如 `100ms`），让客户端更快
  收到 503；不要把它设成 0，否则每次到顶都会立刻拒绝。

**观察**（`/metrics`）：

```bash
curl -sS 'http://127.0.0.1:9428/metrics' | grep -E \
 '^oboe_(mem_rows|mem_bytes|mem_limit_rows|mem_limit_bytes|ingest_stalled_seconds_total|mem_limit_rejected_total) '
```

- `oboe_mem_rows` / `oboe_mem_bytes` 贴着 `oboe_mem_limit_rows` / `oboe_mem_limit_bytes`
  走 = 正在被限流。
- `oboe_ingest_stalled_seconds_total` 增长 = 写入在等 flush（按线程累计，8 并发各等
  1 s 就是 +8）。
- `oboe_mem_limit_rejected_total` 增长 = 已经开始返回 503。持续增长说明 flush 跟不上，
  应降低写入速率、调大 `-mem-max-*`（先确认内存）或检查磁盘吞吐。

**关停**：`store_close` 先置停标志并唤醒所有等待者（等待者立即返回失败，不会睡满
`-mem-wait`），再等后台线程收尾并做最后一次 flush。进行中的 merge 会在 block 边界
中止（源 part 原样保留，下次启动/下个 tick 重做），因此 SIGTERM 不会被大合并拖成
十几秒。实测 8 s 高写入压测（默认上限）SIGTERM 退出 1.94 s。

## 3. retention（按天删除）

- flag：`-retention <days>`，默认 30；`0` 表示永久保留。
- 后台线程每 60 s 执行一次；启动后第一次循环也会执行（`last_retention` 初值为 0）。
- `cutoff = 今天(UTC) - retention_days`，删除 `day < cutoff` 的整个分区目录，包括
  该分区的 memtable 和所有 part。没有按行删除，也没有“删除查询”。
- 保留粒度是一整天：即使分区里只有一条旧数据，整天的分区都会被删除；当天内的数据
  不受影响。
- `retention_days` 只影响本地清理，不会阻止写入未来时间戳；带未来 `_time` 的行会
  进入对应未来天分区，retention 不会删除它们。

示例：

```bash
# 只保留最近 7 天
./oboe -data /var/lib/oboe -retention 7

# 观察分区目录；cutoff 之前的 8 位天目录应消失
ls -1 /var/lib/oboe/partitions
date -u +%Y%m%d
```

验证：启动并等待一个后台周期（约 60 s，启动后首轮会更快执行），
`ls partitions` 中不应再有 `day < 今天(UTC)-7` 的目录。库程序可以用
`logdb.store_apply_retention(s)` 手工触发（见第 5 节的维护程序）。

恢复旧备份时，如果备份数据比当前 `-retention` 更旧，服务启动后的首轮 retention 会
立即删除这些分区。需要保留时先用 `-retention 0` 启动，确认数据后再改回目标值。

## 4. merge（分区内合并）

后台线程每 10 s 检查一次：某分区 part 数达到 `merge_threshold`（默认 8）时触发合并。

- 如果该分区所有 part 的总行数不超过 `merge_max_rows`（默认 400 万），全部参与；
  否则挑选 `merge_threshold` 个最小的 part。
- 使用每个 part 的游标做 k 路归并，顺序为 `(stream, ts)`；只重写、不去重。
- 新 part 写完后先写 `merged_from`，再在独占锁内替换 part 列表，离开锁后才删除旧
  目录。查询持共享锁，不会看到悬垂指针。
- 合并期间旧 part 和新 part 同时存在，分区磁盘占用会临时接近“旧数据 + 新数据”；
  需要预留额外空间。
- 合并产物仍是不可变 part；启动恢复时根据 `merged_from` 剔除旧输入。

示例：

```bash
DATA=/var/lib/oboe

# 合并前统计 part 数
find "$DATA"/partitions/*/parts -mindepth 1 -maxdepth 1 -type d | wc -l

# 调低阈值方便观察；生产通常保持默认 8
./oboe -data "$DATA" -merge-threshold 4

# 等待后台周期后再次统计
sleep 12
find "$DATA"/partitions/*/parts -mindepth 1 -maxdepth 1 -type d | wc -l
find "$DATA"/partitions -name merged_from | head
```

验证：某个分区的 part 数应从 ≥4 降到更少；对应 `merged_from` 文件存在；旧 part
目录被删除；`/metrics` 的 `oboe_merged_parts_total` 增加，`oboe_parts` 下降。

手工合并用 `logdb.store_merge_all(s)`（见第 5 节的维护程序），适合维护脚本。

注意：

- `-block-rows` 只影响新写入的 part；已有 part 保持原来的块大小。
- 合并会重写整个 part，CPU 和 I/O 都不低；不要在高峰期把 `merge_threshold` 调得过低。
- 合并不会减少 retention 之外的数据，也不会改变总行数（除非源数据本身已被 retention
  删除）。

## 5. 容量估算

单块开销很小，但块数会随 stream 分段增加：

| 组成 | 估算 |
| --- | --- |
| `data.bin` | 每块 zlib（level 1）压缩后的列式数据；压缩率取决于字段重复度和消息内容 |
| `index.bin` | 12 字节头 + 64 字节/块 |
| `bloom.bin` | 128 字节到 32 KiB/块（约 10 bit/唯一 token，向上取 2 的幂） |
| `meta.json` | 约 100–200 字节/part，另有目录项和文件系统块开销 |
| `merged_from` | 每行一个 part id，约 20 字节/被合并 part |

块数近似：

```text
blocks ≈ Σ（每个 stream 连续段）ceil(该段行数 / block_rows)
part_bytes ≈ data_compressed + 64 B × blocks + bloom_bytes + meta.json
```

例：每天 100 万行、单 stream、`block_rows = 8192`，约 123 个块：

- index：约 `12 + 123 × 64 ≈ 7.9 KiB`；
- bloom：每块最少 128 B（约 15.4 KiB），最多 32 KiB（约 3.9 MiB）；
- data：压缩后的大小才是主要部分。

所以“每百万行固定开销”通常在几十 KiB 到几 MiB 之间；真正占空间的是压缩后的日志
正文。不同 `stream_fields` 会让同一个 part 里出现更多 stream 段，块数、bloom 和
index 开销随之上升；高基数标签尤其要控制。

其他容量项：

- **memtable**：每个天分区最多约 `flush_max_bytes`（默认 128 MiB）或
  `flush_max_rows`（20 万行）先到者；同时有多个活跃分区时，内存可能叠加。
- **合并临时空间**：分区内旧 part + 新 part，最坏接近该分区当前大小的 2 倍。
- **retention 上限**：日增字节 × `retention_days`，再加当天尚未 flush/merge 的部分。
- **缓存**：`-cache-size` 是 chunk 预算（按桶均分），`-cache-max-entries` 是近似
  条目上限；实际内存还包括 key、12 字节记录头、map 和大值 FIFO。快照文件另计。
- **队列**：所有 job body + 每个 job 的结构体/堆/map 开销；`-queue-max-jobs` 和
  `-queue-max-job-bytes` 只是上限，不是预分配。

维护程序（统计、flush、合并、retention）：

```odin
package main

import "core:fmt"
import "core:time"

import "oboe:logdb"

DATA_DIR :: "/tmp/oboe_operation"

main :: proc() {
	cfg := logdb.default_store_config()
	cfg.data_path = DATA_DIR
	cfg.retention_days = 7
	cfg.merge_threshold = 4
	s := logdb.store_init(cfg, false)
	defer logdb.store_close(s)

	logdb.store_flush_all(s)
	st := logdb.store_stats(s)
	fmt.printfln(
		"partitions=%d parts=%d mem_rows=%d rows=%d disk_bytes=%d",
		st.partitions,
		st.parts,
		st.mem_rows,
		st.rows,
		st.disk_bytes,
	)
	if st.rows > 0 {
		fmt.printfln("range=%s .. %s", time.unix(st.min_ts / 1_000_000_000, 0), time.unix(st.max_ts / 1_000_000_000, 0))
	}

	logdb.store_merge_all(s)
	logdb.store_apply_retention(s)
	st = logdb.store_stats(s)
	fmt.printfln("after maintenance: parts=%d rows=%d disk_bytes=%d", st.parts, st.rows, st.disk_bytes)
}
```

运行：

```bash
odin run . -collection:oboe=/path/to/oboe
```

空目录时预期输出：

```text
partitions=0 parts=0 mem_rows=0 rows=0 disk_bytes=0
after maintenance: parts=0 rows=0 disk_bytes=0
```

对真实数据运行时，`disk_bytes` 可用于容量核对；再用 `du -sh "$DATA"` 对比，差值
来自 `meta.json`、目录和文件系统开销。

## 6. 备份与恢复

### 6.1 停机/离线备份（推荐）

最稳的做法是停止写入，等 memtable 落盘，再复制整个数据目录：

```bash
DATA=/var/lib/oboe
BACKUP=/backup/oboe-$(date -u +%Y%m%dT%H%M%SZ).tar.gz

# 先让最近一批 memtable 落盘；默认 flush_interval 是 1s
sleep 2
# 停止进程（按你的进程管理方式替换；cmd 收到 SIGTERM 会走优雅停机）
systemctl stop oboe

mkdir -p /backup
tar -C "$(dirname "$DATA")" -czf "$BACKUP" "$(basename "$DATA")"

# 恢复服务
systemctl start oboe
```

验证：

```bash
tar -tzf "$BACKUP" | head
tar -tzf "$BACKUP" | grep -E '/(partitions/[0-9]{8}/parts/p_[0-9a-f]{16}/meta\.json)$' | wc -l
```

应能看到 `partitions/<8 位天>/parts/p_<16 位十六进制>/meta.json`。解包到临时目录后
用 `-retention 0` 启动一个只读端口做核对，确认后再覆盖生产目录。

注意：

- 进程管理工具发送 SIGTERM（或 Ctrl-C）时，`cmd` 会关闭监听并从 `server_run`
  返回，再由 defer 链执行 flush、缓存保存和 `store_close`。`kill -9`、OOM 和第二次
  信号才会跳过清理，所以不要把 `kill -9` 当成正常停机。嵌入程序同样应在信号处理里
  只置停标志或调用 `server_request_stop`，等 `server_run` 返回后再
  `store_close`，不要在信号处理函数里做 flush/加锁/分配。
- 队列没有持久化，备份数据目录不会保存 job。
- 快照文件如果放在数据目录里（`-cache-file` 指向该目录），会一起被打包；否则单独
  复制。

### 6.2 在线复制与文件系统快照

part 是不可变的，磁盘变化只有新增 part、合并替换整个 part 和 retention 删除整个
天分区，因此可以在服务运行时复制 `partitions`。需要接受几个窗口：

- 未 flush 的 memtable 不在磁盘上，在线复制会少最近的数据；
- 合并会在替换后删除旧 part 目录，复制过程中可能遇到目录消失；
- retention 可能删除整个天分区目录，`--delete` 的 rsync 会把备份里的这些天也删掉。

能接受这些窗口时，可循环重试复制：

```bash
DATA=/var/lib/oboe
DEST=/backup/oboe-live/partitions

mkdir -p "$DEST"
# --delete 让备份与源一致；合并删掉的旧 part 也会从备份移除
until rsync -a --delete "$DATA/partitions/" "$DEST/"; do
  sleep 1
done

# 缓存单独保存（需要启动时设置了 -cache-file）
curl -sS -X POST 'http://127.0.0.1:9428/cache/save'
```

验证：`find "$DEST" -name meta.json | wc -l` 应不小于一次完整复制的数量；复制期间
源端新增的 part 可能不在本次结果里，需要再跑一轮。

要求精确时间点时，使用文件系统快照（LVM/ZFS/APFS 等）在快照瞬间复制，而不是
逐个文件 rsync：

```bash
# 示例：ZFS；按实际池/数据集替换
zfs snapshot tank/oboe@backup-$(date -u +%Y%m%dT%H%M%SZ)
```

然后从快照目录复制 `partitions/`。这样 part 集合和 `merged_from` 是同一时刻的，
启动恢复能正确剔除被取代的旧 part。

### 6.3 恢复

```bash
DATA=/var/lib/oboe

# 保留现场，避免恢复失败后无法回退
mv "$DATA" "$DATA".broken-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$DATA"

tar -C "$(dirname "$DATA")" -xzf /backup/oboe-20261026T000000Z.tar.gz

# 先用 retention 0 启动，确认数据不会被立即清掉
./oboe -data "$DATA" -retention 0 -listen 127.0.0.1:9428
```

验证：

```bash
curl -sS -G 'http://127.0.0.1:9428/select/logsql/query' \
  --data-urlencode 'query=* | stats count() as total'

curl -sS 'http://127.0.0.1:9428/metrics' \
  | grep -E '^oboe_(rows|parts|partitions|disk_bytes) '
```

确认 `count()` 与备份前的预期一致后，再按需要设置 `-retention`，重启正式实例。

恢复细节：

- 启动时只打开 `meta.json`/`index.bin`/`bloom.bin`/`data.bin` 都完整的 part；
  不完整或被截断的目录会被删除。恢复前先保留原目录，便于排查。
- `merged_from` 会让启动恢复剔除已被合并的旧 part；因此备份里同时存在旧 part 和
  合并产物也不会重复计数，但**不要只复制某个分区的一部分 part**，否则会丢行。
- 旧备份比当前 retention 更旧时，首轮 retention 会立即删除整个天分区；先用
  `-retention 0` 验证。
- 缓存快照是可选文件：`cache_load_file` 要求桶数、chunk 大小、`max_chunks` 与当前
  `cache-size` / `cache-max-entries` 推导一致，否则加载失败（`cmd` 会打印 not loaded
  并以空缓存启动）。快照不保存 hits/misses 等计数器，加载后计数从 0 开始。
- `-cache-file` 的保存是 `tmp + rename`；`kill -9`、OOM 或关停期间的第二次信号
  会跳过退出保存，此时只保留最近一次周期保存或 `POST /cache/save` 的结果。

## 7. 安全

当前实现**没有鉴权、没有 TLS、没有限流**，HTTP 和队列协议都是明文。所有端点
（写入、查询、缓存读写、`/metrics`、UI）一旦网络可达，任何能连上的人都可以使用。

暴露面：

| 服务 | 默认监听 | 风险 |
| --- | --- | --- |
| HTTP | `0.0.0.0:9428` | 任意写入/查询、读改写缓存、触发快照、读指标和 UI |
| 队列 | `127.0.0.1:11300` | 任意 put/reserve/delete/bury/kick，无认证 |

建议：

1. **改监听地址**：HTTP 用 `-listen 127.0.0.1:9428`，队列保持
   `-queue-listen 127.0.0.1:11300`；只在反向代理或内网网关上暴露。
2. **不需要的功能关掉**：不缓存用 `-no-cache`，不排队用 `-no-queue` 或
   `-queue-listen off`，减少暴露端点。
3. **TLS 和鉴权放在反向代理**：由 nginx/Caddy/云负载均衡终止 TLS，做 Basic
   认证、mTLS、IP 白名单和请求体限制。后端只监听回环地址。
4. **限制请求体**：代理层设置 `client_max_body_size`。oboe 自身对超过 256 MiB 的
   请求返回 `413` 并关闭连接，但代理层更早拒绝可以避免把流量读到后端。
5. **限制查询成本**：`stats` 会扫描全部匹配行；`limit` 上限 1,000,000；没有按用户
   配额。对外暴露查询时，应在代理层做认证、限流，并避免给不可信用户开放大范围
   时间查询。
6. **保护数据目录和备份**：part 和缓存快照都是明文；设置目录权限（例如 `700`），
   备份加密存储，避免把 `/metrics`、UI 或缓存端点暴露到公网。
7. **CORS 不能当鉴权**：响应带 `Access-Control-Allow-Origin: *`，只是浏览器侧
   宽松策略；任何非浏览器客户端都不受它限制。
8. **缓存端点可用于覆盖数据**：`PUT/POST /cache/<key>` 和 `DELETE /cache/<key>`
   没有命名空间隔离；不要把应用私有缓存和不可信客户端共用同一实例。
9. **快照端点有 I/O 成本**：`POST /cache/save` 会写整份缓存，暴露后可被反复触发。

反向代理示例（nginx，TLS + Basic 认证 + body 限制）：

```nginx
server {
    listen 443 ssl;
    server_name oboe.example.com;

    ssl_certificate     /etc/nginx/tls/oboe.crt;
    ssl_certificate_key /etc/nginx/tls/oboe.key;

    client_max_body_size 256m;
    auth_basic           "oboe";
    auth_basic_user_file /etc/nginx/oboe.htpasswd;

    location / {
        proxy_pass http://127.0.0.1:9428;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 120s;
    }
}
```

验证：

```bash
nginx -t -c /etc/nginx/nginx.conf
```

`nginx -t` 应输出 syntax is ok / test is successful；然后用
`curl -k -u user:pass https://oboe.example.com/health` 确认代理和鉴权生效，并确认
直接访问 `http://127.0.0.1:9428` 只在回环地址可用。

补充：

- 队列协议没有 TLS，也不支持用户名密码；需要跨主机时用内网 ACL、VPN 或
  stunnel/反向代理包装 TCP。
- 数据目录里可能有敏感日志和缓存值；备份、快照和磁盘都要按敏感数据管理。
- 没有审计日志记录谁写了什么；需要审计时在代理层记录请求，或在上游应用侧记录。

## 8. 已知限制

| 限制 | 说明 |
| --- | --- |
| 单进程、单数据目录 | 没有文件锁；两个进程同时打开同一目录会互相破坏。没有复制、分片、主从 |
| 无持久化队列 | job 只在内存；进程退出即丢失。需要不丢任务时先写日志库或外部持久队列 |
| 无鉴权/TLS/限流 | 见上一节；必须靠网络隔离或反向代理 |
| retention 按天 | 只能整分区删除；`0` 关闭；不支持按查询删除/更新单行 |
| 合并不是去重 | 只重写 part；不消除重复行，不改变逻辑内容 |
| 查询无二级索引 | 依赖分区/块时间范围、bloom 和逐块解码；`stats` 必须扫描所有匹配行 |
| 写入内存窗口 | 未 flush 的 memtable 在崩溃/`kill -9`/OOM 时丢失；正常 SIGTERM/Ctrl-C 会走 defer 链 flush |
| 启动会删除坏 part | 不完整/截断的 part 直接被删除，不隔离、不自动修复 |
| JSON 字段上限 | 单个 JSON 对象最多保留 128 个普通字段，重复键取最后值 |
| 缓存是整代淘汰 | 不是 LRU；条目上限按桶均分，`entries` 是近似值 |
| 缓存快照有配置耦合 | 改桶几何参数后旧快照会被拒绝；计数器不持久化；跨桶非精确同一时刻 |
| 队列上限 | 默认 100 万 job、单 job 65535 字节、1024 连接；超出时 put 返回 `DRAINING` 或连接被关闭 |
| 请求体上限 | 请求头 64 KiB、请求体 256 MiB；超过时分别返回 `431` / `413` |
| 无 HTTP/2 | 只有 HTTP/1.1、keep-alive、chunked、gzip 请求体 |
| 时间语义 | `_time <= 0` 取写入时刻；未来时间会进入未来天分区；retention 不会清理未来分区 |

## 9. 日常检查清单

- [ ] `GET /health` 返回 `OK`。
- [ ] `/metrics` 的 `oboe_mem_rows` 没有持续增长；`oboe_disk_bytes` 与 `du` 趋势
      一致；`oboe_parts` / `oboe_partitions` 没有异常膨胀。
- [ ] 分区目录数量与 retention 预期一致；没有早于 cutoff 的天目录。
- [ ] 合并后旧 part 目录已删除，`merged_from` 对应的旧 id 不再被查询计数。
- [ ] 备份任务验证过 `tar -tzf` 和临时目录恢复；缓存快照和日志数据一起覆盖。
- [ ] 监听地址只在回环/内网；反向代理的 TLS、鉴权、body 限制和超时生效。
- [ ] 数据目录权限、备份加密和磁盘剩余空间满足合并临时空间需求。
