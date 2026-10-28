package oboe

import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"

// In-memory beanstalkd-compatible job queue: tubes, priorities, delays, TTR,
// reserve/release/bury/kick. Jobs are not persisted across restarts.

QUEUE_DEFAULT_TUBE :: "default"
QUEUE_MAX_TUBE_NAME :: 200
QUEUE_TICK_INTERVAL :: 50 * time.Millisecond
QUEUE_RESERVE_SLICE :: 100 * time.Millisecond
QUEUE_URGENT_PRI :: 1024

Queue_Config :: struct {
	listen:        string,
	max_jobs:      int,
	max_job_bytes: int,
	max_conns:     int,
}

default_queue_config :: proc() -> Queue_Config {
	return Queue_Config{
		listen        = "127.0.0.1:11300",
		max_jobs      = 1_000_000,
		max_job_bytes = 65535,
		max_conns     = 1024,
	}
}

Job_State :: enum {
	READY,
	RESERVED,
	DELAYED,
	BURIED,
}

Job :: struct {
	id:             u64,
	pri:            u32,
	seq:            u64,
	state:          Job_State,
	tube:           ^Tube,
	body:           []u8,
	created_ns:     i64,
	delay_until_ns: i64,
	ttr_ns:         i64,
	deadline_ns:    i64,
	reserver:       u64,
	reserves:       u64,
	timeouts:       u64,
	releases:       u64,
	buries:         u64,
	kicks:          u64,
}

Job_Heap :: struct {
	items: [dynamic]^Job,
}

job_heap_less :: proc(a, b: ^Job, delayed: bool) -> bool {
	if delayed {
		if a.delay_until_ns != b.delay_until_ns {
			return a.delay_until_ns < b.delay_until_ns
		}
		return a.seq < b.seq
	}
	if a.pri != b.pri {
		return a.pri < b.pri
	}
	return a.seq < b.seq
}

heap_push :: proc(h: ^Job_Heap, j: ^Job, delayed: bool) {
	append(&h.items, j)
	i := len(h.items) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if !job_heap_less(h.items[i], h.items[parent], delayed) {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

heap_pop :: proc(h: ^Job_Heap, delayed: bool) -> ^Job {
	n := len(h.items)
	if n == 0 {
		return nil
	}
	top := h.items[0]
	n -= 1
	h.items[0] = h.items[n]
	resize(&h.items, n)
	i := 0
	for {
		l := i*2 + 1
		r := l + 1
		smallest := i
		if l < n && job_heap_less(h.items[l], h.items[smallest], delayed) {
			smallest = l
		}
		if r < n && job_heap_less(h.items[r], h.items[smallest], delayed) {
			smallest = r
		}
		if smallest == i {
			break
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
	return top
}

heap_peek :: #force_inline proc(h: ^Job_Heap) -> ^Job {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

Reserved_Ref :: struct {
	id:          u64,
	deadline_ns: i64,
}

Reserved_Heap :: struct {
	items: [dynamic]Reserved_Ref,
}

reserved_less :: proc(a, b: Reserved_Ref) -> bool {
	if a.deadline_ns != b.deadline_ns {
		return a.deadline_ns < b.deadline_ns
	}
	return a.id < b.id
}

reserved_push :: proc(h: ^Reserved_Heap, r: Reserved_Ref) {
	append(&h.items, r)
	i := len(h.items) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if !reserved_less(h.items[i], h.items[parent]) {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

reserved_pop :: proc(h: ^Reserved_Heap) -> Reserved_Ref {
	n := len(h.items)
	if n == 0 {
		return {}
	}
	top := h.items[0]
	n -= 1
	h.items[0] = h.items[n]
	resize(&h.items, n)
	i := 0
	for {
		l := i*2 + 1
		r := l + 1
		smallest := i
		if l < n && reserved_less(h.items[l], h.items[smallest]) {
			smallest = l
		}
		if r < n && reserved_less(h.items[r], h.items[smallest]) {
			smallest = r
		}
		if smallest == i {
			break
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
	return top
}

Tube :: struct {
	name:           string,
	ready:          Job_Heap,
	delayed:        Job_Heap,
	buried:         [dynamic]^Job,
	ready_n:        int,
	urgent_n:       int,
	reserved_n:     int,
	delayed_n:      int,
	buried_n:       int,
	puts:           u64,
	using_n:        int,
	watching:       int,
	waiting:        int,
	cmd_put:        u64,
	cmd_reserve:    u64,
	cmd_delete:     u64,
	cmd_release:    u64,
	cmd_bury:       u64,
	cmd_kick:       u64,
	cmd_touch:      u64,
	pause_until_ns: i64,
}

Queue :: struct {
	cfg:            Queue_Config,
	mu:             sync.Mutex,
	cond:           sync.Cond,
	tubes:          map[string]^Tube,
	jobs:           map[u64]^Job,
	reserved_heap:  Reserved_Heap,
	reserved_n:     int,
	next_id:        u64,
	next_seq:       u64,
	next_conn:      u64,
	created_ns:     i64,
	stopping:       u32,
	worker:         ^thread.Thread,
	connections:    int,
	total_conns:    u64,
	total_puts:     u64,
	cmd_put:        u64,
	cmd_reserve:    u64,
	cmd_delete:     u64,
	cmd_release:    u64,
	cmd_bury:       u64,
	cmd_kick:       u64,
	cmd_touch:      u64,
	timeouts:       u64,
}

Queue_Put_Result :: enum {
	OK,
	TOO_BIG,
	DRAINING,
}

Queue_Stats :: struct {
	current_jobs: int,
	ready:        int,
	reserved:     int,
	delayed:      int,
	buried:       int,
	urgent:       int,
	connections:  int,
	using_n:      int,
	watching:     int,
	waiting:      int,
	total_jobs:   u64,
	total_connections: u64,
	cmd_put:      u64,
	cmd_reserve:  u64,
	cmd_delete:   u64,
	cmd_release:  u64,
	cmd_bury:     u64,
	cmd_kick:     u64,
	cmd_touch:    u64,
	timeouts:     u64,
}

Tube_Stats :: struct {
	name:              string,
	urgent:            int,
	ready:             int,
	reserved:          int,
	delayed:           int,
	buried:            int,
	total_jobs:        u64,
	using_n:           int,
	watching:          int,
	waiting:           int,
	cmd_put:           u64,
	cmd_reserve:       u64,
	cmd_delete:        u64,
	cmd_release:       u64,
	cmd_bury:          u64,
	cmd_kick:          u64,
	cmd_touch:         u64,
	pause_seconds:     i64,
	pause_time_left:   i64,
}

Job_Stats :: struct {
	id:        u64,
	tube:      string,
	state:     Job_State,
	pri:       u32,
	age:       i64,
	delay:     i64,
	ttr:       i64,
	time_left: i64,
	reserves:  u64,
	timeouts:  u64,
	releases:  u64,
	buries:    u64,
	kicks:     u64,
}

Peek_Mode :: enum {
	ID,
	READY,
	DELAYED,
	BURIED,
}

queue_ns_add :: proc(now, delta: i64) -> i64 {
	if delta <= 0 {
		return now
	}
	if now > i64(0x7fffffffffffffff) - delta {
		return i64(0x7fffffffffffffff)
	}
	return now + delta
}

queue_valid_tube_name :: proc(name: string) -> bool {
	if len(name) == 0 || len(name) > QUEUE_MAX_TUBE_NAME {
		return false
	}
	for i in 0..<len(name) {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		switch c {
		case '-', '+', '/', ';', '.', '$', '_', '(', ')':
			continue
		}
		return false
	}
	return true
}

queue_init :: proc(cfg: Queue_Config) -> ^Queue {
	q := new(Queue)
	q.cfg = cfg
	q.tubes = make(map[string]^Tube)
	q.jobs = make(map[u64]^Job)
	q.next_id = 1
	q.next_conn = 1
	q.created_ns = now_ns()
	queue_tube_locked(q, QUEUE_DEFAULT_TUBE)
	q.worker = thread.create_and_start_with_data(q, queue_worker, self_cleanup = false, name = "oboe-queue")
	return q
}

queue_destroy :: proc(q: ^Queue) {
	sync.atomic_store(&q.stopping, 1)
	if q.worker != nil {
		thread.join(q.worker)
		thread.destroy(q.worker)
		q.worker = nil
	}
	for _, j in q.jobs {
		delete(j.body)
		free(j)
	}
	delete_map(q.jobs)
	for _, t in q.tubes {
		delete(t.name)
		delete(t.ready.items)
		delete(t.delayed.items)
		delete(t.buried)
		free(t)
	}
	delete_map(q.tubes)
	delete(q.reserved_heap.items)
	free(q)
}

// queue_tube_locked requires q.mu to be held.
queue_tube_locked :: proc(q: ^Queue, name: string) -> ^Tube {
	if t, ok := q.tubes[name]; ok {
		return t
	}
	t := new(Tube)
	t.name = strings.clone(name)
	q.tubes[t.name] = t
	return t
}

tube_ready_push :: proc(t: ^Tube, j: ^Job) {
	j.state = .READY
	j.reserver = 0
	j.deadline_ns = 0
	heap_push(&t.ready, j, false)
	t.ready_n += 1
	if j.pri < QUEUE_URGENT_PRI {
		t.urgent_n += 1
	}
}

tube_ready_pop :: proc(t: ^Tube) -> ^Job {
	j := heap_pop(&t.ready, false)
	if j != nil {
		t.ready_n -= 1
		if j.pri < QUEUE_URGENT_PRI {
			t.urgent_n -= 1
		}
	}
	return j
}

tube_delayed_push :: proc(t: ^Tube, j: ^Job) {
	j.state = .DELAYED
	heap_push(&t.delayed, j, true)
	t.delayed_n += 1
}

tube_bury :: proc(t: ^Tube, j: ^Job) {
	j.state = .BURIED
	j.reserver = 0
	j.deadline_ns = 0
	append(&t.buried, j)
	t.buried_n += 1
}

queue_put :: proc(q: ^Queue, tube_name: string, pri: u32, delay_ns, ttr_ns: i64, body: []u8) -> (u64, Queue_Put_Result) {
	if q.cfg.max_job_bytes > 0 && len(body) > q.cfg.max_job_bytes {
		return 0, .TOO_BIG
	}
	// Copy the payload before taking the queue lock: the copy can be large and
	// every producer contends on the same mutex.
	payload := make([]u8, len(body))
	copy(payload, body)

	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	if q.cfg.max_jobs > 0 && len(q.jobs) >= q.cfg.max_jobs {
		delete(payload)
		return 0, .DRAINING
	}
	now := now_ns()
	t := queue_tube_locked(q, tube_name)
	j := new(Job)
	j.id = q.next_id
	q.next_id += 1
	j.pri = pri
	j.seq = q.next_seq
	q.next_seq += 1
	j.tube = t
	j.body = payload
	j.created_ns = now
	j.ttr_ns = ttr_ns
	if delay_ns > 0 {
		j.delay_until_ns = queue_ns_add(now, delay_ns)
		tube_delayed_push(t, j)
	} else {
		tube_ready_push(t, j)
	}
	q.jobs[j.id] = j
	t.puts += 1
	t.cmd_put += 1
	q.total_puts += 1
	q.cmd_put += 1
	sync.cond_broadcast(&q.cond)
	return j.id, .OK
}

// queue_promote_locked moves due delayed jobs to ready. Requires q.mu.
queue_promote_locked :: proc(q: ^Queue, now: i64) -> bool {
	changed := false
	for _, t in q.tubes {
		for {
			j := heap_peek(&t.delayed)
			if j == nil || j.delay_until_ns > now {
				break
			}
			heap_pop(&t.delayed, true)
			t.delayed_n -= 1
			j.delay_until_ns = 0
			tube_ready_push(t, j)
			changed = true
		}
	}
	return changed
}

// queue_expire_locked returns jobs whose TTR elapsed to the ready heap.
// Stale heap entries (after touch/release) are discarded lazily.
queue_expire_locked :: proc(q: ^Queue, now: i64) -> bool {
	changed := false
	for len(q.reserved_heap.items) > 0 {
		ref := q.reserved_heap.items[0]
		j, ok := q.jobs[ref.id]
		if !ok || j.state != .RESERVED || j.deadline_ns != ref.deadline_ns {
			reserved_pop(&q.reserved_heap)
			continue
		}
		if now < ref.deadline_ns {
			break
		}
		reserved_pop(&q.reserved_heap)
		j.timeouts += 1
		q.timeouts += 1
		t := j.tube
		if t.reserved_n > 0 {
			t.reserved_n -= 1
		}
		if q.reserved_n > 0 {
			q.reserved_n -= 1
		}
		tube_ready_push(t, j)
		changed = true
	}
	if len(q.reserved_heap.items) > 2*q.reserved_n + 1024 {
		clear(&q.reserved_heap.items)
		for _, j in q.jobs {
			if j.state == .RESERVED {
				reserved_push(&q.reserved_heap, Reserved_Ref{id = j.id, deadline_ns = j.deadline_ns})
			}
		}
	}
	return changed
}

queue_worker :: proc(data: rawptr) {
	q := cast(^Queue)data
	for sync.atomic_load(&q.stopping) == 0 {
		sync.mutex_lock(&q.mu)
		now := now_ns()
		changed := queue_promote_locked(q, now)
		if queue_expire_locked(q, now) {
			changed = true
		}
		if changed {
			sync.cond_broadcast(&q.cond)
		}
		sync.mutex_unlock(&q.mu)
		time.sleep(QUEUE_TICK_INTERVAL)
	}
}

queue_reserve_into :: proc(q: ^Queue, watched: []string, conn_id: u64, timeout_ns: i64, body: ^Buf) -> (u64, bool) {
	now := now_ns()
	deadline := i64(0)
	if timeout_ns > 0 {
		deadline = queue_ns_add(now, timeout_ns)
	}
	sync.mutex_lock(&q.mu)
	defer {
		for name in watched {
			if t, ok := q.tubes[name]; ok && t.waiting > 0 {
				t.waiting -= 1
			}
		}
		sync.mutex_unlock(&q.mu)
	}
	for name in watched {
		if t, ok := q.tubes[name]; ok {
			t.waiting += 1
		}
	}
	for {
		now = now_ns()
		queue_promote_locked(q, now)
		queue_expire_locked(q, now)
		best: ^Tube = nil
		for name in watched {
			t, ok := q.tubes[name]
			if !ok {
				continue
			}
			if t.pause_until_ns > now {
				continue
			}
			if heap_peek(&t.ready) == nil {
				continue
			}
			if best == nil || job_heap_less(heap_peek(&t.ready), heap_peek(&best.ready), false) {
				best = t
			}
		}
		if best != nil {
			j := tube_ready_pop(best)
			j.state = .RESERVED
			j.reserver = conn_id
			j.deadline_ns = queue_ns_add(now, j.ttr_ns)
			j.reserves += 1
			best.reserved_n += 1
			best.cmd_reserve += 1
			q.cmd_reserve += 1
			q.reserved_n += 1
			reserved_push(&q.reserved_heap, Reserved_Ref{id = j.id, deadline_ns = j.deadline_ns})
			buf_reset(body)
			buf_write(body, j.body)
			return j.id, true
		}
		if timeout_ns == 0 {
			return 0, false
		}
		wait := QUEUE_RESERVE_SLICE
		if timeout_ns > 0 {
			remaining := deadline - now
			if remaining <= 0 {
				return 0, false
			}
			if remaining < i64(QUEUE_RESERVE_SLICE) {
				wait = time.Duration(remaining)
			}
		}
		sync.cond_wait_with_timeout(&q.cond, &q.mu, wait)
	}
}

queue_delete :: proc(q: ^Queue, id: u64, conn_id: u64) -> bool {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	j, ok := q.jobs[id]
	if !ok || j.state != .RESERVED || j.reserver != conn_id {
		return false
	}
	t := j.tube
	if t.reserved_n > 0 {
		t.reserved_n -= 1
	}
	if q.reserved_n > 0 {
		q.reserved_n -= 1
	}
	t.cmd_delete += 1
	q.cmd_delete += 1
	delete_key(&q.jobs, id)
	delete(j.body)
	free(j)
	return true
}

queue_release :: proc(q: ^Queue, id: u64, conn_id: u64, pri: u32, delay_ns: i64) -> bool {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	j, ok := q.jobs[id]
	if !ok || j.state != .RESERVED || j.reserver != conn_id {
		return false
	}
	t := j.tube
	if t.reserved_n > 0 {
		t.reserved_n -= 1
	}
	if q.reserved_n > 0 {
		q.reserved_n -= 1
	}
	j.pri = pri
	j.releases += 1
	t.cmd_release += 1
	q.cmd_release += 1
	if delay_ns > 0 {
		j.delay_until_ns = queue_ns_add(now_ns(), delay_ns)
		tube_delayed_push(t, j)
	} else {
		tube_ready_push(t, j)
	}
	sync.cond_broadcast(&q.cond)
	return true
}

queue_bury :: proc(q: ^Queue, id: u64, conn_id: u64, pri: u32, set_pri: bool) -> bool {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	j, ok := q.jobs[id]
	if !ok || j.state != .RESERVED || j.reserver != conn_id {
		return false
	}
	t := j.tube
	if t.reserved_n > 0 {
		t.reserved_n -= 1
	}
	if q.reserved_n > 0 {
		q.reserved_n -= 1
	}
	if set_pri {
		j.pri = pri
	}
	j.buries += 1
	t.cmd_bury += 1
	q.cmd_bury += 1
	tube_bury(t, j)
	return true
}

queue_touch :: proc(q: ^Queue, id: u64, conn_id: u64) -> bool {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	j, ok := q.jobs[id]
	if !ok || j.state != .RESERVED || j.reserver != conn_id {
		return false
	}
	j.deadline_ns = queue_ns_add(now_ns(), j.ttr_ns)
	reserved_push(&q.reserved_heap, Reserved_Ref{id = j.id, deadline_ns = j.deadline_ns})
	j.tube.cmd_touch += 1
	q.cmd_touch += 1
	return true
}

queue_kick :: proc(q: ^Queue, tube_name: string, bound: int) -> int {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	t, ok := q.tubes[tube_name]
	if !ok || bound <= 0 || len(t.buried) == 0 {
		return 0
	}
	kicked := 0
	kept: [dynamic]^Job
	for j in t.buried {
		if kicked < bound {
			t.buried_n -= 1
			j.kicks += 1
			tube_ready_push(t, j)
			kicked += 1
		} else {
			append(&kept, j)
		}
	}
	delete(t.buried)
	t.buried = kept
	if kicked > 0 {
		t.cmd_kick += 1
		q.cmd_kick += 1
		sync.cond_broadcast(&q.cond)
	}
	return kicked
}

queue_kick_job :: proc(q: ^Queue, id: u64) -> bool {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	j, ok := q.jobs[id]
	if !ok || j.state != .BURIED {
		return false
	}
	t := j.tube
	for k in 0..<len(t.buried) {
		if t.buried[k] == j {
			for m in k..<len(t.buried)-1 {
				t.buried[m] = t.buried[m+1]
			}
			resize(&t.buried, len(t.buried)-1)
			break
		}
	}
	if t.buried_n > 0 {
		t.buried_n -= 1
	}
	j.kicks += 1
	tube_ready_push(t, j)
	t.cmd_kick += 1
	q.cmd_kick += 1
	sync.cond_broadcast(&q.cond)
	return true
}

queue_peek_into :: proc(q: ^Queue, tube_name: string, mode: Peek_Mode, id: u64, body: ^Buf) -> (u64, bool) {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	j: ^Job = nil
	switch mode {
	case .ID:
		j, _ = q.jobs[id]
	case .READY:
		if t, ok := q.tubes[tube_name]; ok {
			j = heap_peek(&t.ready)
		}
	case .DELAYED:
		if t, ok := q.tubes[tube_name]; ok {
			j = heap_peek(&t.delayed)
		}
	case .BURIED:
		if t, ok := q.tubes[tube_name]; ok {
			for cand in t.buried {
				if j == nil || job_heap_less(cand, j, false) {
					j = cand
				}
			}
		}
	}
	if j == nil {
		return 0, false
	}
	buf_reset(body)
	buf_write(body, j.body)
	return j.id, true
}

queue_pause_tube :: proc(q: ^Queue, tube_name: string, seconds: int) -> bool {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	t, ok := q.tubes[tube_name]
	if !ok {
		return false
	}
	if seconds <= 0 {
		t.pause_until_ns = 0
	} else {
		t.pause_until_ns = queue_ns_add(now_ns(), i64(seconds)*1_000_000_000)
	}
	return true
}

queue_stats :: proc(q: ^Queue) -> Queue_Stats {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	st := Queue_Stats{
		current_jobs = len(q.jobs),
		connections  = q.connections,
		total_jobs   = q.total_puts,
		total_connections = q.total_conns,
		cmd_put      = q.cmd_put,
		cmd_reserve  = q.cmd_reserve,
		cmd_delete   = q.cmd_delete,
		cmd_release  = q.cmd_release,
		cmd_bury     = q.cmd_bury,
		cmd_kick     = q.cmd_kick,
		cmd_touch    = q.cmd_touch,
		timeouts     = q.timeouts,
	}
	for _, t in q.tubes {
		st.ready += t.ready_n
		st.urgent += t.urgent_n
		st.reserved += t.reserved_n
		st.delayed += t.delayed_n
		st.buried += t.buried_n
		st.using_n += t.using_n
		st.watching += t.watching
		st.waiting += t.waiting
	}
	return st
}

queue_tube_stats :: proc(q: ^Queue, tube_name: string) -> (Tube_Stats, bool) {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	t, ok := q.tubes[tube_name]
	if !ok {
		return {}, false
	}
	now := now_ns()
	st := Tube_Stats{
		name         = t.name,
		urgent       = t.urgent_n,
		ready        = t.ready_n,
		reserved     = t.reserved_n,
		delayed      = t.delayed_n,
		buried       = t.buried_n,
		total_jobs   = t.puts,
		using_n      = t.using_n,
		watching     = t.watching,
		waiting      = t.waiting,
		cmd_put      = t.cmd_put,
		cmd_reserve  = t.cmd_reserve,
		cmd_delete   = t.cmd_delete,
		cmd_release  = t.cmd_release,
		cmd_bury     = t.cmd_bury,
		cmd_kick     = t.cmd_kick,
		cmd_touch    = t.cmd_touch,
	}
	if t.pause_until_ns > now {
		st.pause_time_left = (t.pause_until_ns - now + 999_999_999) / 1_000_000_000
		st.pause_seconds = st.pause_time_left
	}
	return st, true
}

queue_job_stats :: proc(q: ^Queue, id: u64) -> (Job_Stats, bool) {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	j, ok := q.jobs[id]
	if !ok {
		return {}, false
	}
	now := now_ns()
	st := Job_Stats{
		id       = j.id,
		tube     = j.tube.name,
		state    = j.state,
		pri      = j.pri,
		age      = (now - j.created_ns) / 1_000_000_000,
		ttr      = j.ttr_ns / 1_000_000_000,
		reserves = j.reserves,
		timeouts = j.timeouts,
		releases = j.releases,
		buries   = j.buries,
		kicks    = j.kicks,
	}
	switch j.state {
	case .RESERVED:
		st.time_left = max(0, (j.deadline_ns - now + 999_999_999) / 1_000_000_000)
	case .DELAYED:
		st.delay = max(0, (j.delay_until_ns - now + 999_999_999) / 1_000_000_000)
		st.time_left = st.delay
	case .READY, .BURIED:
	}
	return st, true
}

queue_list_tubes :: proc(q: ^Queue, allocator := context.allocator) -> []string {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	names := make([]string, len(q.tubes), allocator)
	i := 0
	for name in q.tubes {
		names[i] = name
		i += 1
	}
	return names
}

queue_new_conn :: proc(q: ^Queue) -> u64 {
	id := sync.atomic_add(&q.next_conn, 1)
	return id
}

queue_conn_opened :: proc(q: ^Queue) -> bool {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	if q.cfg.max_conns > 0 && q.connections >= q.cfg.max_conns {
		return false
	}
	q.connections += 1
	q.total_conns += 1
	return true
}

queue_use :: proc(q: ^Queue, from, to: string) {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	if len(from) > 0 {
		if t, ok := q.tubes[from]; ok && t.using_n > 0 {
			t.using_n -= 1
		}
	}
	queue_tube_locked(q, to).using_n += 1
}

queue_watch :: proc(q: ^Queue, name: string) {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	queue_tube_locked(q, name).watching += 1
}

queue_unwatch :: proc(q: ^Queue, name: string) {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	if t, ok := q.tubes[name]; ok && t.watching > 0 {
		t.watching -= 1
	}
}

// queue_conn_closed returns every job still reserved by the connection to the
// ready state, as beanstalkd does when a client disconnects.
queue_conn_closed :: proc(q: ^Queue, used: string, watched: []string, reserved: []u64) {
	sync.mutex_lock(&q.mu)
	defer sync.mutex_unlock(&q.mu)
	changed := false
	for id in reserved {
		j, ok := q.jobs[id]
		if !ok || j.state != .RESERVED {
			continue
		}
		t := j.tube
		if t.reserved_n > 0 {
			t.reserved_n -= 1
		}
		if q.reserved_n > 0 {
			q.reserved_n -= 1
		}
		tube_ready_push(t, j)
		changed = true
	}
	if len(used) > 0 {
		if t, ok := q.tubes[used]; ok && t.using_n > 0 {
			t.using_n -= 1
		}
	}
	for name in watched {
		if t, ok := q.tubes[name]; ok && t.watching > 0 {
			t.watching -= 1
		}
	}
	if q.connections > 0 {
		q.connections -= 1
	}
	if changed {
		sync.cond_broadcast(&q.cond)
	}
}
