# 以库形式使用 oboe

`src/` 是一个完整的 Odin 包（`package oboe`），日志存储/查询、缓存、任务队列和
HTTP 服务器都可以脱离命令行单独使用。`src/main.odin` 只是附带的可执行入口：导入
该包时其中的 `main` 过程不会被调用。

示例程序见 [`examples/embed/main.odin`](../examples/embed/main.odin)：

```bash
odin run examples/embed
```

## 1. 引入

最直接的方式是把仓库作为子模块或直接拷贝到项目里，然后使用相对路径导入：

```odin
import oboe "../../third_party/oboe/src"
```

也可以使用 Odin 的 collection，避免依赖相对层级：

```bash
odin build . -collection:oboe=/path/to/oboe
```

```odin
import oboe "oboe:src"
```

包目录名是 `src`，默认标识符会变成 `src`，因此请显式起别名 `oboe`。不要使用
`import . "..."` 点导入，否则 `oboe.main` 会与你自己程序的 `main` 重名（普通别名
导入不会冲突）。

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
cfg := oboe.default_store_config()
cfg.data_path = "/var/lib/oboe"
cfg.stream_fields = []string{"level", "service"}
```

## 3. 存储与 LogsQL 查询

```odin
cfg := oboe.default_store_config()
cfg.data_path = "/tmp/oboe_embed"
cfg.stream_fields = []string{"level", "service"}
cfg.flush_interval = 1 << 62 // 示例里手动 flush；实际使用保留默认 1s

s := oboe.store_init(cfg, false) // false = 不启动后台 flush/merge 线程
defer oboe.store_close(s)

arena: oboe.Arena
defer oboe.arena_destroy(&arena)

parsed: oboe.Parsed_Line
line := `{"_time":"2026-10-26T10:00:00Z","level":"error","service":"api","_msg":"boom"}`
oboe.parse_json_line(line, &arena, "_msg", "_time", &parsed)
ts, _ := oboe.parse_iso_ns(parsed.time_raw)

fields := make([]oboe.Field, parsed.nfields)
defer delete(fields)
copy(fields, parsed.fields[:parsed.nfields])

rows := [1]oboe.Ingest_Row{{
	ts     = ts,
	stream = oboe.compute_stream_id(fields, cfg.stream_fields),
	msg    = parsed.msg,
	fields = fields,
}}
oboe.store_ingest(s, rows[:])
oboe.store_flush_all(s)

q, ok := oboe.parse_query_string("level:error | limit 10", oboe.now_ns())
if !ok {
	// 查询语法错误
	return
}
defer oboe.query_destroy(&q)

res := oboe.execute_query(s, &q, 100)
defer oboe.result_destroy(&res)
for i in 0..<len(res.rows) {
	row := &res.rows[i]
	service, _ := oboe.out_row_get(row, "service")
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
cfg := oboe.default_cache_config()
cfg.max_bytes = 64 << 20
cfg.max_entries = 100_000

c := oboe.cache_init(cfg)
defer oboe.cache_destroy(c)

oboe.cache_set(c, "greeting", transmute([]u8)string("hello"), i64(30 * time.Second))

out := oboe.buf_init(64)
defer oboe.buf_destroy(&out)
if oboe.cache_get_into(c, "greeting", &out) {
	fmt.println(string(out.data[:]))
}

st := oboe.cache_stats(c) // entries/bytes/hits/.../collisions
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
os.make_directory_all("/tmp/oboe_cache")
if bytes, ok := oboe.cache_save_file(c, "/tmp/oboe_cache/cache.bin"); ok {
	fmt.printfln("saved %d bytes", bytes)
}

loaded, ok := oboe.cache_load_file(cfg, "/tmp/oboe_cache/cache.bin")
if ok {
	defer oboe.cache_destroy(loaded)
}
```

- `cache_load_file` 会用 `cfg` 重新推导桶几何参数，并要求与文件头一致；配置改变后
  旧文件会被拒绝加载。加载会丢弃已过期条目。
- 保存是 `tmp + rename` 原子替换，每桶独立 zlib 压缩；保存期间缓存仍可读写，但跨桶
  不保证精确同一时刻。
- 需要周期性保存时，可以复用 CLI 同样的机制（`Cache_Saver` + `cache_saver_run`）：

```odin
saver := oboe.Cache_Saver{
	c        = c,
	path     = "/tmp/oboe_cache/cache.bin", // 需在 saver 存活期间保持有效
	interval = time.Minute,
}
t := thread.create_and_start_with_data(&saver, oboe.cache_saver_run, self_cleanup = false)
defer {
	sync.atomic_store(&saver.stop, 1)
	thread.join(t)
	thread.destroy(t)
}
```

（需要 `core:thread`、`core:sync`、`core:time`。）

- 进程被信号直接杀死时不会执行退出保存；依赖周期保存或自行在退出路径调用
  `cache_save_file`。HTTP 服务还提供 `POST /cache/save`（需设置 `srv.cache_file`）。

## 5. 任务队列

`queue_init` 会启动一个后台线程推进延迟 job 和 TTR 超时，`queue_destroy` 会 join 它。

```odin
q := oboe.queue_init(oboe.default_queue_config())
defer oboe.queue_destroy(q)

id, res := oboe.queue_put(q, "default", 0, 0, i64(30 * time.Second), transmute([]u8)string("body"))

conn := oboe.queue_new_conn(q)
body := oboe.buf_init(64)
defer oboe.buf_destroy(&body)
if job, ok := oboe.queue_reserve_into(q, []string{"default"}, conn, 0, &body); ok {
	fmt.println(job, string(body.data[:]))
	oboe.queue_delete(q, job, conn)
}
oboe.queue_conn_closed(q, "default", nil, nil)
```

- `queue_put` 的 `delay_ns` / `ttr_ns` 均为纳秒；`timeout_ns = 0` 时 reserve 不阻塞。
- `queue_conn_opened` / `queue_use` / `queue_watch` / `queue_conn_closed` 用于模拟
  beanstalkd 客户端状态；只做内存队列时可以省略，但断线回收依赖
  `queue_conn_closed` 把该连接的 reserved job 放回 ready。
- 队列只存内存，无持久化与复制；`queue_stats` / `queue_tube_stats` / `queue_job_stats`
  返回同样的统计结构。

## 6. HTTP 服务器

```odin
srv := oboe.server_init(oboe.default_server_config(), s, c, q)
srv.cache_file = "/tmp/oboe_cache/cache.bin" // 可选：启用 POST /cache/save
if !oboe.server_run(srv) {
	oboe.server_destroy(srv)
	os.exit(1)
}
oboe.server_destroy(srv) // server_run 返回后释放
```

- `server_run` 阻塞，内部创建 worker 线程；从其他线程调用 `server_stop(srv)` 可以
  关闭监听并 join worker。`server_run` 返回后调用 `server_destroy(srv)` 关闭排队中
  的连接并释放 `Server` 本身；对未启动或已停止的 server 也可安全调用。
- `cache` 和 `queue` 参数可以为 `nil`，对应 API 会返回 503。
- 不需要真实端口时，可以直接调用 `dispatch(srv, &req, &arena)` 处理构造好的
  `Http_Request`；测试就是这样做的（见 `src/handlers_test.odin`）。
- 服务器不安装信号处理器；需要在收到 SIGINT/SIGTERM 时优雅退出，请在宿主程序中
  自行处理并调用 `server_stop`，再释放各组件与缓存快照。

## 7. 内存所有权一览

| 数据 | 由谁释放 |
| --- | --- |
| `store_ingest` 的入参 | 调用方；store 会拷贝 |
| `parse_json_line` 产生的字符串 | 传入的 `Arena`，用 `arena_reset`/`arena_destroy` 释放 |
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

- `src/main.odin` 与 CLI 参数解析也在库包内，但只有构建可执行文件时才会被执行。
- 缓存快照格式带版本号与几何参数校验，升级/改配置后旧文件可能无法加载。
- 缓存 `cache_stats` 需要扫描有效条目（O(entries)），不要在高频路径反复调用。
- 队列与缓存默认都只驻留内存；缓存快照是可选能力，队列没有持久化。
