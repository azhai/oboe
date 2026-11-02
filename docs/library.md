# 以库形式使用 oboe

源码拆成多个 Odin 包（`common`、`netio`、`logdb`、`cache`、`queue`、`web`），包布局与
依赖方向见 [模块与包布局](modules.md)。日志存储/查询、缓存、任务队列和 HTTP 服务器
都可以脱离命令行单独使用。`cmd/` 是唯一可执行入口（`package main`，只做装配与关停），
库包本身不包含 `main`。

可运行示例：

```bash
odin run examples/embed # store + query + cache + queue
odin run examples/logs  # 写入 + LogsQL 查询
odin run examples/cache # TTL、get_into、快照
odin run examples/queue # put/reserve/delete + 断线回收
```

## 1. 引入

最直接的方式是把仓库作为子模块或直接拷贝到项目里，然后按需使用相对路径导入：

```odin
import "../../third_party/oboe/common" // 包名 common
import "../../third_party/oboe/netio"  // 单独使用 TCP 行读取时
import "../../third_party/oboe/logdb"  // 包名 logdb
import "../../third_party/oboe/cache"
import "../../third_party/oboe/queue"
import "../../third_party/oboe/web"
```

也可以使用 Odin 的 collection，避免依赖相对层级：

```bash
odin build . -collection:oboe=/path/to/oboe
```

```odin
import "oboe:common"
import "oboe:netio"
import "oboe:logdb"
import "oboe:cache"
import "oboe:queue"
import "oboe:web"
```

包名与目录名一致（`common`、`logdb`、`cache`、`queue`、`web`、`netio`）。如果局部
变量与包名同名（例如 `cache` / `queue`），请给 import 起别名，例如
`import cachelib "oboe:cache"`。不要使用 `import . "..."` 点导入，否则包内符号会
与你自己程序的标识符冲突。

## 2. 组件速查

| 组件 | 初始化 | 销毁 | 主要入口 |
| --- | --- | --- | --- |
| 存储 | `store_init(cfg, start_worker)` | `store_close(s)` | `store_ingest`、`store_flush_all`、`store_stats` |
| 查询 | `parse_query_string` | `query_destroy(&q)` | `execute_query`、`result_destroy` |
| 缓存 | `cache_init(cfg)` | `cache_destroy(c)` | `cache_set/get_into/delete/stats` |
| 缓存快照（可选） | `cache_load_file(cfg, path)` | `cache_destroy` | `cache_save_file`、`Cache_Saver` |
| 队列 | `queue_init(cfg)` | `queue_destroy(q)` | `queue_put/reserve_into/delete/...` |
| HTTP 服务 | `server_init(cfg, store, cache, queue)` | `server_destroy(srv)` | `server_run`、`dispatch` |

所有配置都可以从 `default_*_config()` 开始改，例如：

```odin
import "oboe:logdb"

cfg := logdb.default_store_config()
cfg.data_path = "/var/lib/oboe"
cfg.stream_fields = []string{"level", "service"}
```

## 3. 存储与 LogsQL 查询

```odin
import "oboe:logdb"
import "oboe:common"
import "core:fmt"

cfg := logdb.default_store_config()
cfg.data_path = "/tmp/oboe_embed"
cfg.stream_fields = []string{"level", "service"}
cfg.flush_interval = 1 << 62 // 示例里手动 flush；实际使用保留默认 1s

s := logdb.store_init(cfg, false) // false = 不启动后台 flush/merge 线程
defer logdb.store_close(s)

arena: common.Arena
defer common.arena_destroy(&arena)

parsed: logdb.Parsed_Line
line := `{"_time":"2026-10-26T10:00:00Z","level":"error","service":"api","_msg":"boom"}`
logdb.parse_json_line(line, &arena, "_msg", "_time", &parsed)
ts, _ := common.parse_iso_ns(parsed.time_raw)

fields := make([]logdb.Field, parsed.nfields)
defer delete(fields)
copy(fields, parsed.fields[:parsed.nfields])

rows := [1]logdb.Ingest_Row{{
	ts     = ts,
	stream = logdb.compute_stream_id(fields, cfg.stream_fields),
	msg    = parsed.msg,
	fields = fields,
}}
logdb.store_ingest(s, rows[:])
logdb.store_flush_all(s)

q, ok := logdb.parse_query_string("level:error | limit 10", common.now_ns())
if !ok {
	// 查询语法错误
	return
}
defer logdb.query_destroy(&q)

res := logdb.execute_query(s, &q, 100)
defer logdb.result_destroy(&res)
for i in 0..<len(res.rows) {
	row := &res.rows[i]
	service, _ := logdb.out_row_get(row, "service")
	fmt.println(row.msg, service)
}
```

要点：

- `store_ingest` 会把 `msg` 和 `fields` 拷贝进分区 arena，返回后即可释放入参。
- `start_worker = false` 时由调用方负责调用 `store_flush_due` / `store_flush_all` /
  `store_merge_all`；生产环境通常保持 `true`，让后台线程按 `flush_interval` 工作。
- `parse_query_string` 的第二个参数是“现在”，用于解析 `_time:5m` 等相对时间。
- `Query` 和 `Query_Result` 各自持有 arena，必须分别调用 `query_destroy` 和
  `result_destroy`；结果里的字符串只在 `result_destroy` 之前有效。
- `execute_query` 未指定 limit 时使用 `DEFAULT_QUERY_LIMIT`（1000）。
- `store_apply_retention(s)` 可手动执行过期分区清理，`store_stats(s)` 返回分区/parts/
  行数/磁盘字节快照。

## 4. 缓存（落盘可选）

默认就是纯内存缓存；不调用快照相关函数时不会产生任何文件 I/O。

```odin
import "oboe:cache"
import "oboe:common"
import "core:fmt"
import "core:time"

cfg := cache.default_cache_config()
cfg.max_bytes = 64 << 20
cfg.max_entries = 100_000

c := cache.cache_init(cfg)
defer cache.cache_destroy(c)

cache.cache_set(c, "greeting", transmute([]u8)string("hello"), i64(30 * time.Second))

out := common.buf_init(64)
defer common.buf_destroy(&out)
if cache.cache_get_into(c, "greeting", &out) {
	fmt.println(string(out.data[:]))
}

st := cache.cache_stats(c) // entries/bytes/hits/.../collisions
```

- `ttl_ns > 0` 设置过期时间，`0` 表示不过期；过期项在读取或 `cache_stats` 时清理。
- `cache_get` 返回调用方拥有的副本（用完 `delete`）；`cache_get_into` 写入你自己的
  `Buf`，适合热路径复用缓冲区。
- 缓存对象在 `cache_init` 时固定当前 `context.allocator`。条目可能由任意线程淘汰，
  因此请确保该 allocator 的生命周期覆盖缓存，并在同一 allocator 下销毁。
- 值以副本存储；单条 value 超过 chunk（约 64 KiB）会进入桶内大值 FIFO。
- 读路径持桶的共享锁，写/删除持独占锁，可直接多线程访问。

### 可选的快照

只有显式调用以下 API 时才落盘（目标目录需要已存在）：

```odin
import "oboe:cache"
import "core:fmt"
import "core:os"

os.make_directory_all("/tmp/oboe_cache")
if bytes, ok := cache.cache_save_file(c, "/tmp/oboe_cache/cache.bin"); ok {
	fmt.printfln("saved %d bytes", bytes)
}

loaded, ok := cache.cache_load_file(cfg, "/tmp/oboe_cache/cache.bin")
if ok {
	defer cache.cache_destroy(loaded)
}
```

- `cache_load_file` 会用 `cfg` 重新推导桶几何参数，并要求文件头里的
  `bucket_count` / `max_chunks` 与当前配置一致；改变桶数或每桶 chunk 数的配置
  （例如大幅调整 `-cache-size`，或把 `-cache-max-entries` 调到足以改变桶数的低值）
  会拒绝加载旧文件，其他参数变化通常不影响。加载会丢弃已过期条目。
- 保存是 `tmp + rename` 原子替换，每桶独立 zlib 压缩；保存期间缓存仍可读写，但跨桶
  不保证精确同一时刻。
- 需要周期性保存时，可以复用 CLI 同样的机制（`Cache_Saver` + `cache_saver_run`）：

```odin
saver := cache.Cache_Saver{
	c        = c,
	path     = "/tmp/oboe_cache/cache.bin", // 需在 saver 存活期间保持有效
	interval = time.Minute,
}
t := thread.create_and_start_with_data(&saver, cache.cache_saver_run, self_cleanup = false)
defer {
	sync.atomic_store(&saver.stop, 1)
	thread.join(t)
	thread.destroy(t)
}
```

（需要 `core:thread`、`core:sync`、`core:time`。）

- 库本身不安装信号处理器；宿主进程被 `SIGINT`/`SIGTERM` 直接杀死时不会执行退出
  保存。`cmd/main.odin` 已在 CLI 层处理信号；嵌入使用时请自行在退出路径调用
  `cache_save_file`，或依赖周期保存（`Cache_Saver`）/`POST /cache/save`。

## 5. 任务队列

`queue_init` 会启动一个后台线程推进延迟 job 和 TTR 超时，`queue_destroy` 会 join 它。

```odin
import "oboe:queue"
import "oboe:common"
import "core:fmt"
import "core:time"

q := queue.queue_init(queue.default_queue_config())
defer queue.queue_destroy(q)

id, res := queue.queue_put(q, "default", 0, 0, i64(30 * time.Second), transmute([]u8)string("body"))
fmt.println("put:", id, res)

if !queue.queue_conn_opened(q) {
	// max_conns 已满（默认 1024）
	return
}
conn := queue.queue_new_conn(q)

body := common.buf_init(64)
defer common.buf_destroy(&body)

reserved: [dynamic]u64
defer delete(reserved)

if job, ok := queue.queue_reserve_into(q, []string{"default"}, conn, 0, &body); ok {
	fmt.println(job, string(body.data[:]))
	append(&reserved, job) // 自行记录该连接持有中的 id
	queue.queue_delete(q, job, conn)
}
// queue_conn_closed 只回收显式传入的 reserved 列表：已 delete 的 id 不在
// RESERVED 状态会被跳过；若 worker 在 delete 前退出，列表里的 job 回到 ready。
queue.queue_conn_closed(q, "default", nil, reserved[:])
```

- `queue_put` 的 `delay_ns` / `ttr_ns` 均为纳秒；`timeout_ns = 0` 时 reserve 不阻塞。
- `queue_conn_opened` / `queue_use` / `queue_watch` / `queue_conn_closed` 维护
  beanstalkd 风格的连接统计。只做内存队列时可以省略这些调用，但**断线回收不是
  自动的**：`queue_conn_closed` 只把 `reserved: []u64` 里仍处于 `RESERVED` 的 job
  放回 ready。TCP 服务端为每个连接维护自己的 reserved 列表（`queue/queue_server.odin`
  的 `QConn.reserved`）；自行嵌入时也要自己记录 reserve 过的 id，`used` / `watched`
  仅用于扣减统计计数。
- 队列只存内存，无持久化与复制；`queue_stats` / `queue_tube_stats` / `queue_job_stats`
  返回同样的统计结构。

## 6. HTTP 服务器

```odin
import "oboe:web"
import "core:os"

srv := web.server_init(web.default_server_config(), s, c, q)
srv.cache_file = "/tmp/oboe_cache/cache.bin" // 可选：启用 POST /cache/save
if !web.server_run(srv) {
	web.server_destroy(srv)
	os.exit(1)
}
web.server_destroy(srv) // server_run 返回后释放
```

- `server_run` 阻塞，内部创建 worker 线程；从其他线程调用 `server_stop(srv)` 可以
  关闭监听并 join worker。`server_run` 返回后调用 `server_destroy(srv)` 关闭排队中
  的连接并释放 `Server` 本身；对未启动或已停止的 server 也可安全调用。
- `cache` 和 `queue` 参数可以为 `nil`，对应 API 会返回 503。
- 不需要真实端口时，可以直接调用 `dispatch(srv, &req, &arena)` 处理构造好的
  `Http_Request`；测试就是这样做的（见 `web/handlers_test.odin`）。
- 服务器不安装信号处理器；`SIGINT`/`SIGTERM` 默认直接终止进程，`defer` 不会执行。
  `cmd/main.odin` 在 CLI 层捕获信号并完成优雅关停（见 [运维](cookbook/operations.md)
  的「关停」）。嵌入使用时请在宿主程序中自行处理：信号处理器里只做原子置位并调用
  `server_request_stop(srv)`（异步信号安全，不 join 线程），然后在主线程调用
  `server_stop(srv)` join worker，再依次停止保存线程、`store_close`（flush
  memtable）、`cache_save_file`（如需快照），最后销毁各组件。

## 7. 内存所有权一览

| 数据 | 由谁释放 |
| --- | --- |
| `store_ingest` 的入参 | 调用方；store 会拷贝 |
| `parse_json_line` 产生的字符串 | 未转义部分是输入字符串的视图；转义部分在传入的 `Arena` |
| `Query` | `query_destroy` |
| `Query_Result` | `result_destroy` |
| `cache_get` 的返回值 | 调用方 `delete` |
| `cache_get_into` | 写入调用方的 `Buf` |
| `queue_reserve_into` | 写入调用方的 `Buf`，job body 仍归队列 |
| `cache_load_file` 返回的缓存 | `cache_destroy` |

## 8. 并发与生命周期

- `Store` 内部用读写锁保护分区与 memtable，`store_ingest` 可并发调用。
- 缓存每桶一把读写锁，可多线程并发读写；统计计数是原子量。
- 队列用一把互斥锁 + 条件变量，reserve 在超时切片内等待，不自旋。
- 各组件互不依赖：可以只用缓存、只用队列，或单独使用查询/存储模块；HTTP 服务通过
  参数接收 store/cache/queue，不依赖全局状态。
- 销毁顺序：先停 HTTP/队列网络服务，再停保存线程，最后销毁 cache/queue/store。

## 9. 注意事项

- `cmd/main.odin` 是可执行入口（`package main`），不能也不应被库使用者导入；库包
  本身不含 `main`。
- 缓存快照格式带版本号与几何参数校验，升级/改配置后旧文件可能无法加载。
- 缓存 `cache_stats` 需要扫描有效条目（O(entries)），不要在高频路径反复调用。
- 队列与缓存默认都只驻留内存；缓存快照是可选能力，队列没有持久化。
