# 任务队列（beanstalkd 兼容）

队列是 beanstalkd 协议子集的内存实现，默认监听 `127.0.0.1:11300`。
可以使用任意 beanstalkd 客户端，也可以直接用 `nc` 敲命令。

## 命令表

| 命令 | 说明 | 成功 | 失败 |
| --- | --- | --- | --- |
| `put <pri> <delay> <ttr> <bytes>` + body | 放入 job | `INSERTED <id>` | `EXPECTED_CRLF`、`JOB_TOO_BIG`、`DRAINING`、`BAD_FORMAT` |
| `use <tube>` | 选择 put 的 tube | `USING <tube>` | `BAD_FORMAT` |
| `watch <tube>` | 加入 reserve 的 watch 列表 | `WATCHING <n>` | `BAD_FORMAT` |
| `ignore <tube>` | 移除 watch；最后一个不能 ignore | `WATCHING <n>` | `NOT_IGNORED` |
| `reserve` | 阻塞等待 job | `RESERVED <id> <bytes>` + body | 无（一直阻塞） |
| `reserve-with-timeout <s>` | 带超时等待 | 同上 | `TIMED_OUT` |
| `delete <id>` | 删除自己保留的 job | `DELETED` | `NOT_FOUND` |
| `release <id> <pri> <delay>` | 放回 ready/delayed | `RELEASED` | `NOT_FOUND` |
| `bury <id> [pri]` | 埋藏 | `BURIED` | `NOT_FOUND` |
| `touch <id>` | 延长 TTR 截止时间 | `TOUCHED` | `NOT_FOUND` |
| `kick <n>` | 把 buried 移回 ready，最多 n 个 | `KICKED <n>` | - |
| `kick-job <id>` | 指定 job 移回 ready | `KICKED` | `NOT_FOUND` |
| `pause-tube <tube> <s>` | 暂停出队 s 秒；0 取消 | `PAUSED` | `NOT_FOUND` |
| `peek <id>` | 按 id 看 job | `FOUND <id> <bytes>` + body | `NOT_FOUND` |
| `peek-ready` / `peek-delayed` / `peek-buried` | 看队首 | 同上 | `NOT_FOUND` |
| `stats` | 全局统计 | `OK <n>` + YAML | - |
| `stats-tube <tube>` | tube 统计 | 同上 | `NOT_FOUND` |
| `stats-job <id>` | job 统计 | 同上 | `NOT_FOUND` |
| `list-tubes` | 全部 tube | `OK <n>` + YAML | - |
| `list-tube-used` | 当前 use 的 tube | `USING <tube>` | - |
| `list-tubes-watched` | watch 列表 | `OK <n>` + YAML | - |
| `quit` | 关闭连接 | - | - |

未知命令返回 `UNKNOWN_COMMAND`。`reserve-with-timeout 0` 等同于非阻塞的
`reserve`（立刻 `TIMED_OUT` 或返回 job）。`put` 的 `delay` / `ttr` 单位是秒。

## `nc` 快速上手

```bash
# 启动服务（另开终端）
./oboe -data /tmp/oboe_queue_demo -queue-listen 127.0.0.1:11300

# 放入一个 job：优先级 0，无延迟，TTR 60s，body 5 字节
printf 'put 0 0 60 5\r\nhello\r\n' | nc 127.0.0.1 11300
# => INSERTED 1

# 取走并删除
printf 'reserve-with-timeout 2\r\ndelete 1\r\n' | nc 127.0.0.1 11300
# => RESERVED 1 5
#    hello
#    DELETED
```

逐行交互示例：

```text
$ nc 127.0.0.1 11300
put 0 0 60 5
hello
INSERTED 1
use emails
USING emails
put 10 0 60 6
jobbody
INSERTED 2
watch emails
WATCHING 2
reserve-with-timeout 5
RESERVED 2 6
jobbody
bury 2
BURIED
kick 1
KICKED 1
reserve-with-timeout 5
RESERVED 2 6
jobbody
delete 2
DELETED
```

注意每条命令和 body 都以 `\r\n` 结尾；`nc` 交互输入时换行即可。

## 语义要点

- **状态机**：`READY → RESERVED`；`RESERVED` 可 `delete`（终结）、`release`
  （回 ready 或延迟后回 ready）、`bury`（进 buried，可被 `kick` 拉回 ready）。
- **优先级与 FIFO**：tube 内按 `(优先级, 序号)` 出队，优先级数字越小越先；
  `pri < 1024` 记为 urgent（只影响 `stats` 里的 `current-jobs-urgent`）。
- **delay**：`put`/`release` 的 `delay > 0` 先进 DELAYED，到期后自动进 ready；
  后台线程每 50 ms 推进一次。
- **TTR**：reserve 开始计时，超过 `ttr` 秒未 `delete`/`release`/`bury`/`touch`，
  job 自动回到 ready 并计入 `job-timeouts`；`touch` 把截止时间重新设为
  `now + ttr`。TTR 最小生效粒度与后台节拍（50 ms）相同。
- **断线回收**：客户端断开时，该连接 reserved 的 job 全部回到 ready，与
  beanstalkd 一致；已删除的不受影响。
- **pause-tube**：暂停期间该 tube 不出队；到期自动恢复。
- **上限**：`-queue-max-jobs`（默认 100 万）满时 `put` 返回 `DRAINING`；
  body 超过 `-queue-max-job-bytes`（默认 65535）返回 `JOB_TOO_BIG`。
  单个连接数上限 `max_conns`（1024），超出时新连接被直接关闭。
- **无持久化**：job 只存内存，进程退出即丢失；无复制、无主从。
- tube 名规则：长度 1–200，字符限 `[A-Za-z0-9]` 与 `- + / ; . $ _ ( )`。

## Go / Python 客户端片段

beanstalkd 协议是纯文本，任何能写 TCP 的语言都可以直接用；也可以引入现成的
beanstalk 客户端库，只要它不依赖未实现命令。

Go（使用 github.com/beanstalkd/go-beanstalk）：

```go
c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
if err != nil { panic(err) }
defer c.Close()

id, err := c.Put([]byte("hello"), 0, 0, 60*time.Second)
if err != nil { panic(err) }

job, err := c.Reserve(5 * time.Second)
if err != nil { panic(err) }
_ = job.Body
c.Delete(job.Id)

// 断线回收演示：若拿到 job 后不 delete 直接 Close，job 会回到 ready
_ = id
```

Python（使用 greenstalk 或 beanstalkc）：

```python
import greenstalk

client = greenstalk.Client(("127.0.0.1", 11300))
try:
    jid = client.put(b"hello", priority=0, delay=0, ttr=60)
    job = client.reserve(timeout=5)   # 返回 Job，job.id / job.body
    client.delete(job)
finally:
    client.close()

# 不 delete 直接 close：该 job 回到 ready，可被其他 worker 领走
```


库内 API 也可以直接用（不需要端口），见 [嵌入到程序](embedding.md) 的队列部分。

## worker 模式

一个典型的生产-消费循环：

```
producer:  put(pri, delay, ttr, body)      -> INSERTED id
worker:    reserve-with-timeout <s>        -> RESERVED id bytes
           try handle(body)
           成功  -> delete id
           暂时失败-> release id pri delay （或 bury id 交由人工/告警处理）
           处理超时-> touch id 续期，继续处理
           崩溃/断开 -> 连接关闭，job 自动回到 ready
```

建议：

- `ttr` 设为预期最长处理时间，宁可大一点，配合 `touch` 续期，避免误超时。
- 处理失败先 `release` 带延迟重试，多次失败再 `bury`，由运维 `kick`。
- worker 需要优雅退出：处理完当前 job 再断开，避免无谓的断线重投。
- 若 job 必须不丢失，请自行持久化（例如先写日志库再 put），因为队列本身无持久化。

## 统计 YAML

`stats` 返回 beanstalkd 风格 YAML（键为 camelCase，与 beanstalkd 一致）：

```text
OK 278
---
current-jobs-urgent: 0
current-jobs-ready: 1
current-jobs-reserved: 0
current-jobs-delayed: 0
current-jobs-buried: 0
current-connections: 1
current-jobs: 1
total-jobs: 3
current-using: 1
current-watching: 1
current-waiting: 0
cmd-put: 3
cmd-reserve: 2
cmd-delete: 1
cmd-release: 0
cmd-bury: 0
cmd-kick: 0
cmd-touch: 0
job-timeouts: 0
total-connections: 1
max-job-size: 65535
uptime: 42
pid: 1234
version: "1.13"
```

`stats-tube` 多出 `name`、`pause`、`pause-time-left`；`stats-job` 输出
`id`、`tube`、`state`、`pri`、`age`、`delay`、`ttr`、`time-left`、`reserves`、
`timeouts`、`releases`、`buries`、`kicks`。`state` 取 `READY` / `RESERVED` /
`DELAYED` / `BURIED`。

Prometheus 侧 `/metrics` 提供 `oboe_queue_jobs/ready/reserved/delayed/buried/
connections/puts_total`。
