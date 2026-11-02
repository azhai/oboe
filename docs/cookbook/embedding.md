# 嵌入到程序

`common`、`netio`、`logdb`、`cache`、`queue`、`web` 都是普通库包，`cmd/` 里的
`main` 过程只有在构建可执行文件时才会编译。日志存储、查询、缓存、队列和 HTTP 服务
可以按需组合，下面的片段都是完整程序，不依赖 `./data` 之外的仓库文件。

## 1. 引入与构建

仓库内可以直接用相对路径，导入路径相对于当前 `.odin` 文件：

```odin
import "../../common"
import "../../logdb"
import cachelib "../../cache"
import queuelib "../../queue"
import "../../web"
```

仓库外或独立项目建议使用 collection，导入写成 `oboe:<包名>`：

```odin
import "oboe:common"
import "oboe:logdb"
import cachelib "oboe:cache"
import queuelib "oboe:queue"
import "oboe:web"
```

```bash
# 使用 collection：所有构建/运行/检查命令都要带同一个 -collection 参数
odin check . -collection:oboe=/path/to/oboe
odin run . -collection:oboe=/path/to/oboe

# 仓库内相对导入不需要 collection
odin run examples/embed
```

约定：

- 包名与目录名一致：`common`、`netio`、`logdb`、`cache`、`queue`、`web`。
  局部变量叫 `cache` 或 `queue` 时，给 import 起别名（如上例的 `cachelib`、
  `queuelib`），不要用 `import .` 点导入。
- `-collection:oboe=...` 的值可以指向仓库根目录；不要在导入里再写 `/oboe/`。
- 下面的完整片段使用 `oboe:` 形式，仓库内运行时只改 import 行即可。

## 2. 日志：写入 + LogsQL 查询

```odin
package main

import "core:fmt"
import "core:os"

import "oboe:common"
import "oboe:logdb"

DATA_DIR :: "/tmp/oboe_embed_store"

main :: proc() {
	os.remove_all(DATA_DIR)
	defer os.remove_all(DATA_DIR)

	cfg := logdb.default_store_config()
	cfg.data_path = DATA_DIR
	cfg.stream_fields = []string{"level", "service"}
	cfg.flush_interval = 1 << 62
	s := logdb.store_init(cfg, false)
	defer logdb.store_close(s)

	arena: common.Arena
	defer common.arena_destroy(&arena)

	line := `{"_time":"2026-10-26T10:00:00Z","level":"error","service":"api","_msg":"boom"}`
	parsed: logdb.Parsed_Line
	if !logdb.parse_json_line(line, &arena, "_msg", "_time", &parsed) {
		fmt.eprintln("invalid json line")
		os.exit(1)
	}
	ts, tok := common.parse_iso_ns(parsed.time_raw)
	if !tok {
		fmt.eprintln("invalid _time")
		os.exit(1)
	}

	fields := make([]logdb.Field, parsed.nfields)
	defer delete(fields)
	copy(fields, parsed.fields[:parsed.nfields])

	rows := [1]logdb.Ingest_Row{{
		ts     = ts,
		stream = logdb.compute_stream_id(fields, cfg.stream_fields),
		msg    = parsed.msg,
		fields = fields,
	}}
	fmt.printfln("ingested=%d", logdb.store_ingest(s, rows[:]))
	logdb.store_flush_all(s)

	q, ok := logdb.parse_query_string("level:error | fields _time, _msg, service | limit 10", common.now_ns())
	if !ok {
		fmt.eprintln("invalid query")
		os.exit(1)
	}
	defer logdb.query_destroy(&q)

	res := logdb.execute_query(s, &q, 10)
	defer logdb.result_destroy(&res)
	fmt.printfln("matched=%d scanned=%d pruned_blocks=%d", res.matched_rows, res.scanned_rows, res.pruned_blocks)
	for i in 0..<len(res.rows) {
		row := &res.rows[i]
		service, _ := logdb.out_row_get(row, "service")
		fmt.printfln("%s service=%s", row.msg, service)
	}
}
```

运行：

```bash
odin run . -collection:oboe=/path/to/oboe
```

预期输出：

```text
ingested=1
matched=1 scanned=1 pruned_blocks=0
boom service=api
```

要点：

- `parse_json_line` 对未转义的字符串返回输入 `line` 的切片视图，只有带转义的值和
  解析出的键才分配在传入的 `Arena`；因此结果的有效期取 `line` 与 arena 两者的较小
  值。`fields` 只是把 `Field`（名称/值两个视图）复制成调用方数组，不复制字符串内容。
- `store_ingest` 会把 `msg` 和 `fields` 再拷贝进分区自己的 arena，返回后即可释放
  入参；调用方只负责 `fields` 数组本身。
- `store_init(cfg, false)` 不启动后台 flush/merge 线程，所以示例显式调用
  `store_flush_all`。生产程序通常传 `true`，让后台线程按 `flush_interval` 落盘。
- `parse_query_string` 的第二个参数是“现在”，用于 `_time:5m` 等相对时间。
- `Query` 和 `Query_Result` 各自持有 arena，必须分别 `query_destroy`、
  `result_destroy`；结果中的字符串只在 `result_destroy` 之前有效。
- `execute_query` 的第三个参数是默认 limit；查询串里写了 `limit` 时以查询串为准，
  上限 `MAX_QUERY_LIMIT`（1,000,000）。

## 3. 缓存：TTL / get_into / 快照

```odin
package main

import "core:fmt"
import "core:os"
import "core:time"

import "oboe:common"
import cachelib "oboe:cache"

DATA_DIR :: "/tmp/oboe_embed_cache"
SNAPSHOT :: "/tmp/oboe_embed_cache/cache.bin"

main :: proc() {
	os.remove_all(DATA_DIR)
	defer os.remove_all(DATA_DIR)
	os.make_directory_all(DATA_DIR)

	cfg := cachelib.default_cache_config()
	cfg.max_bytes = 8 << 20
	cfg.max_entries = 10_000
	cfg.max_value = 1 << 20
	c := cachelib.cache_init(cfg)
	defer cachelib.cache_destroy(c)

	cachelib.cache_set(c, "greeting", transmute([]u8)string("hello"), i64(30 * time.Second))
	cachelib.cache_set(c, "forever", transmute([]u8)string("stays"), 0)

	out := common.buf_init(64)
	defer common.buf_destroy(&out)
	if cachelib.cache_get_into(c, "greeting", &out) {
		fmt.printfln("cache: greeting=%s", out.data[:])
	}

	if value, ok := cachelib.cache_get(c, "forever"); ok {
		defer delete(value)
		fmt.printfln("cache: forever=%s", value)
	}

	fmt.printfln("cache: deleted=%v", cachelib.cache_delete(c, "forever"))
	st := cachelib.cache_stats(c)
	fmt.printfln("cache: entries=%d bytes=%d hits=%d misses=%d", st.entries, st.bytes, st.hits, st.misses)

	if bytes, ok := cachelib.cache_save_file(c, SNAPSHOT); ok {
		fmt.printfln("cache: saved=%d bytes", bytes)
	}
	if loaded, ok := cachelib.cache_load_file(cfg, SNAPSHOT); ok {
		defer cachelib.cache_destroy(loaded)
		if value, gok := cachelib.cache_get(loaded, "greeting"); gok {
			defer delete(value)
			fmt.printfln("cache: reloaded=%s", value)
		}
	}
}
```

运行：

```bash
odin run . -collection:oboe=/path/to/oboe
```

预期输出（`saved` 的字节数随压缩结果略有差异）：

```text
cache: greeting=hello
cache: forever=stays
cache: deleted=true
cache: entries=1 bytes=5 hits=2 misses=0
cache: saved=14543 bytes
cache: reloaded=hello
```

要点：

- `ttl_ns > 0` 是绝对过期时间（纳秒），`0` 表示不过期；不是滑动过期。
- `cache_get_into` 会**追加**写入调用方的 `Buf`，不会清空它；复用前调用
  `common.buf_reset(&out)`。
- `cache_get` 返回调用方拥有的副本，用完 `delete`；`cache_load_file` 返回一个新的
  缓存对象，用 `cache_destroy` 释放。
- 快照目录必须已存在；保存写 `<path>.tmp` 后 `rename`。加载要求桶数、chunk 大小、
  `max_chunks` 与当前 `cfg` 推导出的几何参数一致，否则返回失败（调用方可以继续用
  空缓存启动）。
- 快照只保存条目，不保存 `hits`/`misses`/`sets` 等计数器；加载后计数从 0 开始。
- `cache_init` 固定当前 `context.allocator`；缓存条目可能由其它线程淘汰，因此该
  allocator 必须线程安全、生命周期覆盖缓存，并在同一 allocator 下 `cache_destroy`。

## 4. 队列：put / reserve / delete / 断线回收

```odin
package main

import "core:fmt"
import "core:time"

import "oboe:common"
import queuelib "oboe:queue"

main :: proc() {
	q := queuelib.queue_init(queuelib.default_queue_config())
	defer queuelib.queue_destroy(q)

	conn := queuelib.queue_new_conn(q)
	if !queuelib.queue_conn_opened(q) {
		fmt.eprintln("queue: connection limit reached")
		return
	}

	id, put := queuelib.queue_put(
		q,
		queuelib.QUEUE_DEFAULT_TUBE,
		0,
		0,
		i64(30 * time.Second),
		transmute([]u8)string("first job"),
	)
	fmt.printfln("queue: put id=%d result=%v", id, put)

	body := common.buf_init(64)
	defer common.buf_destroy(&body)

	job, ok := queuelib.queue_reserve_into(q, []string{queuelib.QUEUE_DEFAULT_TUBE}, conn, 0, &body)
	if !ok {
		fmt.eprintln("queue: reserve failed")
		return
	}
	fmt.printfln("queue: reserved id=%d body=%s", job, body.data[:])

	// 模拟 worker 断线：不 delete，交给 queue_conn_closed 放回 ready。
	queuelib.queue_conn_closed(
		q,
		queuelib.QUEUE_DEFAULT_TUBE,
		[]string{queuelib.QUEUE_DEFAULT_TUBE},
		[]u64{job},
	)

	conn2 := queuelib.queue_new_conn(q)
	if !queuelib.queue_conn_opened(q) {
		fmt.eprintln("queue: connection limit reached")
		return
	}
	job2, ok2 := queuelib.queue_reserve_into(q, []string{queuelib.QUEUE_DEFAULT_TUBE}, conn2, 0, &body)
	if !ok2 {
		fmt.eprintln("queue: job was not recovered")
		return
	}
	fmt.printfln("queue: recovered id=%d body=%s", job2, body.data[:])
	fmt.printfln("queue: delete=%v", queuelib.queue_delete(q, job2, conn2))
	queuelib.queue_conn_closed(q, queuelib.QUEUE_DEFAULT_TUBE, []string{queuelib.QUEUE_DEFAULT_TUBE}, nil)
}
```

运行：

```bash
odin run . -collection:oboe=/path/to/oboe
```

预期输出：

```text
queue: put id=1 result=OK
queue: reserved id=1 body=first job
queue: recovered id=1 body=first job
queue: delete=true
```

要点：

- `queue_put` 的 `delay_ns` / `ttr_ns` 是纳秒；`pri` 越小越优先。body 会被复制，
  返回后调用方可以释放入参。
- `queue_reserve_into` 先 `buf_reset` 再写入 body；`timeout_ns = 0` 表示不阻塞，
  找不到 job 立即返回 `false`。job body 仍归队列，调用方只拥有 `Buf`。
- `queue_new_conn` 只生成连接 id，不增加连接计数；真正模拟客户端连接时配合
  `queue_conn_opened` / `queue_conn_closed`。
- **进程内 API 不会自动记录某个连接 reserve 了哪些 job**。断线回收必须像上面这样把
  该连接 reserved 的 id 列表传给 `queue_conn_closed`；如果只传 `nil`，job 会一直
  保持 `RESERVED`，直到 TTR 到期由后台线程放回 ready。
- `queue_init` 启动后台线程推进 delay 和 TTR；`queue_destroy` 会 join 它。队列只存
  内存，进程退出即丢失。

## 5. HTTP 服务：init / run / stop

```odin
package main

import "core:fmt"
import "core:os"
import "core:thread"
import "core:time"

import "oboe:logdb"
import cachelib "oboe:cache"
import queuelib "oboe:queue"
import "oboe:web"

DATA_DIR :: "/tmp/oboe_embed_web"

Stop_Args :: struct {
	srv:   ^web.Server,
	after: time.Duration,
}

stop_later :: proc(data: rawptr) {
	args := cast(^Stop_Args)data
	time.sleep(args.after)
	web.server_stop(args.srv)
}

main :: proc() {
	os.remove_all(DATA_DIR)
	defer os.remove_all(DATA_DIR)

	store_cfg := logdb.default_store_config()
	store_cfg.data_path = DATA_DIR
	store_cfg.stream_fields = []string{"level", "service"}
	store := logdb.store_init(store_cfg)
	defer logdb.store_close(store)

	cache := cachelib.cache_init(cachelib.default_cache_config())
	defer cachelib.cache_destroy(cache)

	queue := queuelib.queue_init(queuelib.default_queue_config())
	defer queuelib.queue_destroy(queue)

	server_cfg := web.default_server_config()
	server_cfg.listen = "127.0.0.1:0" // 随机端口，避免与并行测试冲突
	server_cfg.workers = 2
	server_cfg.stream_fields = store_cfg.stream_fields

	srv := web.server_init(server_cfg, store, cache, queue)
	defer web.server_destroy(srv)

	args := Stop_Args{srv = srv, after = time.Second}
	t := thread.create_and_start_with_data(&args, stop_later, self_cleanup = false)
	defer {
		thread.join(t)
		thread.destroy(t)
	}

	if !web.server_run(srv) {
		fmt.eprintln("web: server did not start")
		os.exit(1)
	}
	fmt.println("web: server stopped")
}
```

运行：

```bash
odin run . -collection:oboe=/path/to/oboe
```

预期输出：

```text
oboe: listening on http://127.0.0.1:0 (workers=2, data=/tmp/oboe_embed_web)
web: server stopped
```

实际绑定的端口在 `srv.bound_port`。需要请求端点时，可以在停止线程里先打印这个端口，
再用它访问 `http://127.0.0.1:<bound_port>/health`；不要假设配置里的 `:0` 就是实际端口。

要点：

- `server_init(cfg, store, cache, queue)` 只保存指针，不拥有组件；`cache`、`queue`
  可以传 `nil`，对应端点返回 `503`，`store` 必须是有效指针。
- `server_run` **阻塞**：内部监听、创建 HTTP worker，并在循环里 `accept`。从另一个
  线程调用 `server_stop(srv)` 会关闭监听并 join 所有 worker；不要在 worker 内部
  调用 `server_stop`（会等待自己，形成死锁）。
- 正常流程是 `server_run` 返回后调用 `server_destroy(srv)`；它关闭排队中的连接并
  释放 `Server` 本身。对未启动或已停止的 server 也可以安全调用。
- `server_init` 之后再设置 `srv.cfg.stream_fields` 或 `srv.cache_file` 是允许的。
  `stream_fields` 应和 `store_cfg.stream_fields` 一致；`cache_file` 非空时启用
  `POST /cache/save`。
- 服务器本身不安装信号处理器。嵌入程序若需要 SIGINT/SIGTERM 优雅退出，应自己捕获
  信号，但信号处理里只做 async-signal-safe 的动作（`server_request_stop` 只置标志并
  关闭监听）。等 `server_run` 返回后再依次 `server_stop`、`queue_server_stop`、停止
  保存线程、`cache_save_file`，最后销毁各组件；不要在信号处理函数里 join 线程或分配
  内存。`cmd/main.odin` 就是这样做的：第一次信号只置位并关闭监听，`server_run`
  返回后由 `defer` 链完成 flush、缓存保存和销毁；第二次信号直接 `_exit`。
- 同一个 `Server` 只调用一次 `server_run`；重复启动会重新监听并创建 worker。

## 6. 用 dispatch 测试处理器

```odin
package main

import "core:fmt"
import "core:os"

import "oboe:common"
import "oboe:logdb"
import "oboe:web"

DATA_DIR :: "/tmp/oboe_embed_dispatch"

main :: proc() {
	os.remove_all(DATA_DIR)
	defer os.remove_all(DATA_DIR)

	cfg := logdb.default_store_config()
	cfg.data_path = DATA_DIR
	cfg.stream_fields = []string{"level", "service"}
	cfg.flush_interval = 1 << 62
	store := logdb.store_init(cfg, false)
	defer logdb.store_close(store)

	srv := web.server_init(web.default_server_config(), store)
	srv.cfg.stream_fields = cfg.stream_fields
	defer web.server_destroy(srv)

	arena: common.Arena
	defer common.arena_destroy(&arena)

	ingest := web.Http_Request{
		method  = "POST",
		path    = "/insert/jsonline",
		version = "HTTP/1.1",
		body    = transmute([]u8)string(`{"_time":"2026-10-26T10:00:00Z","level":"error","service":"api","_msg":"boom"}`),
	}
	resp := web.dispatch(srv, &ingest, &arena)
	fmt.printfln("ingest: code=%d body=%s", resp.code, resp.body.data[:])
	common.buf_destroy(&resp.body)

	query := web.Http_Request{
		method  = "GET",
		path    = "/select/logsql/query",
		query   = "query=level%3Aerror&limit=10",
		version = "HTTP/1.1",
	}
	resp = web.dispatch(srv, &query, &arena)
	fmt.printfln("query: code=%d body=%s", resp.code, resp.body.data[:])
	common.buf_destroy(&resp.body)
	logdb.store_flush_all(store)
}
```

运行：

```bash
odin run . -collection:oboe=/path/to/oboe
```

预期输出：

```text
ingest: code=200 body={"rows":1,"errors":0}
query: code=200 body={"_time":"2026-10-26T10:00:00Z","_msg":"boom","level":"error","service":"api"}
```

要点：

- 不用打开端口：构造 `Http_Request` 后直接调用 `dispatch`。`query` 中的参数需要
  URL 编码（`level:error` 写成 `level%3Aerror`），`dispatch` 会解码。
- `dispatch` 的响应体是 `Http_Response.body`，由调用方 `common.buf_destroy`；
  `resp.extra` 里是状态行之外的响应头（每行以 CRLF 结尾）。`resp.body` 使用普通
  allocator，不归传入的 `Arena` 管理。
- `arena` 持有解码后的参数等临时数据；每次调用应传入自己的 arena，并在用完后
  `arena_destroy`，不要把 arena 分配的对象带出它的生命周期。
- 测试缓存端点时可以在 `server_init` 后设置 `srv.cache_file`；不设置时
  `POST /cache/save` 返回 `400`。

## 7. 生命周期与销毁顺序

推荐的启动/停止顺序：

```text
启动：
  store_init
  cache_init / cache_load_file
  queue_init
  queue_server_start（可选）
  cache_saver 线程（可选）
  server_init
  server_run（阻塞）

停止：
  server_run 返回 / server_stop
  server_destroy
  queue_server_stop
  停止 cache_saver 线程
  cache_save_file（需要落盘时）
  cache_destroy
  queue_destroy
  store_close
```

- `store_close` 会先停后台线程、`store_flush_all`，再关闭所有 part。它必须最后调用；
  在它之后不能再调用 `store_ingest` / 查询。
- `queue_server_stop` 会把队列的 `stopping` 置位并 join acceptor；此后队列后台线程
  也会退出，队列不能继续使用。先停服务端，再 `queue_destroy`。
- `server_destroy` 不释放 store/cache/queue，只释放 `Server` 和排队连接；组件在它
  之后按上面的顺序销毁。
- 如果不用 HTTP 服务，就省略 server/queue_server；只用 cache 的程序可以只
  `cache_init` / `cache_destroy`。
- 任何组件都不应在仍有其它线程读写时销毁。尤其是 `cache_destroy` 与
  `store_close`：先停 HTTP、队列连接和保存线程。

## 8. allocator 与所有权

| 数据 | 由谁释放 |
| --- | --- |
| `store_ingest` 的入参 `msg` / `fields` | 调用方；store 会复制 |
| `parse_json_line` 产生的字符串 | 未转义部分是输入字符串的视图；转义部分与解析出的键在传入的 `Arena` |
| `Parsed_Line.fields` 数组 | 结构体内的固定数组，随结构体失效；需要跨调用保留时自行复制 |
| `Query` | `query_destroy` |
| `Query_Result`、结果行和字符串 | `result_destroy` |
| `cache_get` 的返回值 | 调用方 `delete` |
| `cache_get_into` | 调用方的 `Buf`，且是追加写入 |
| `cache_load_file` 返回的缓存 | `cache_destroy` |
| `queue_put` 的 body | 队列复制；调用方保留自己的入参 |
| `queue_reserve_into` 的 body | 调用方的 `Buf`；job body 仍归队列 |
| `queue_server_start` / `server_init` 返回的对象 | `queue_server_stop` / `server_destroy` |
| `web.dispatch` 的响应体 | `common.buf_destroy(&resp.body)` |
| `dispatch` 的解码参数 | 传入的 `Arena` |

allocator 注意事项：

- `Cache` 在 `cache_init` 时固定当前 `context.allocator`，用于 chunk、key、value 和
  大值链表；条目可能由任意线程淘汰。请使用线程安全的 allocator（默认进程堆即可），
  并保证它比缓存活得更久。
- `Store` 的 memtable arena 在写入线程分配、由 flush/merge 后台线程释放；`Queue`
  的 body 也可能在生产者线程分配、由消费者或后台线程释放。因此不要在调用点临时
  切换到单线程的 per-task allocator；跨线程长期存活的数据应使用进程级线程安全
  allocator（测试里用 `runtime.default_allocator()` 就是这个原因）。
- `cache_saver_run` 的 `path` 字符串必须在 saver 存活期间保持有效；`cmd` 用
  `strings.clone` 保证这一点。

## 9. 并发约定

- **存储**：`store_ingest` 可并发调用；查询持共享读锁完成整个扫描；flush、merge、
  retention 的元数据变更持独占锁。不要在查询/写入进行中 `store_close`。
- **缓存**：每桶一把读写锁；`cache_get_into` 持共享锁，`cache_set`/`cache_delete`/
  `cache_stats` 持独占锁。可多线程并发读写，但 `cache_stats` 是 O(entries)，不要在
  高频路径或每次请求都调用。
- **队列**：一把互斥锁 + 条件变量；reserve 在 100 ms 切片内等待，TTR/delay 由后台
  线程推进。`queue_destroy` 前必须停止所有消费者和队列 TCP 服务。
- **HTTP**：`dispatch` 本身不依赖 socket，多个线程可以并发调用；但同一个
  `Http_Request` 不应被并发修改。`server_stop` 可从任意非 worker 线程调用。
- **快照**：`cache_save_file` 持 `save_mu` 串行化，保存期间缓存仍可读写；快照不保证
  跨桶的精确同一时刻一致，但每个桶内部自洽。

## 10. 相对导入 vs collection

| 方式 | 写法 | 命令 | 适用 |
| --- | --- | --- | --- |
| 相对导入 | `import "../../logdb"` | `odin run .` | 仓库内示例、vendored 子目录 |
| collection | `import "oboe:logdb"` | `odin run . -collection:oboe=/path/to/oboe` | 独立项目、包层级会变化 |

注意：

- 相对路径以**导入文件所在目录**为基准；把程序移动层级后要同步改路径。
- collection 的 `-collection` 参数必须出现在每次 `odin build` / `odin run` /
  `odin check` / `odin test` 命令中，否则会报找不到 `oboe:*`。
- 不要导入 `cmd`：它是 `package main`，只用于构建可执行文件；库代码只导入
  `common`/`logdb`/`cache`/`queue`/`web`。
- 包名与局部变量冲突时用别名（`cachelib "oboe:cache"`），不要用点导入。
