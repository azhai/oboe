package tests

// End-to-end tests for the beanstalk-protocol queue server over real TCP.
// Every test binds an ephemeral port (127.0.0.1:0), reads Queue_Server's
// bound_port back through testutil's Queue_Test_Env, and drives the server
// with the same wire protocol external clients use.

import "base:runtime"
import "core:fmt"
import "core:strings"
import "core:testing"
import "core:time"

// queue_e2e_reserve sends reserve-with-timeout and reads the RESERVED reply.
queue_e2e_reserve :: proc(c: ^Test_Conn, timeout_s := 0) -> (id: u64, body: string, ok: bool) {
	if !test_tcp_write_line(c, fmt.tprintf("reserve-with-timeout %d", timeout_s)) {
		return 0, "", false
	}
	return test_queue_read_job(c, "RESERVED")
}

queue_e2e_delete :: proc(t: ^testing.T, c: ^Test_Conn, id: u64) {
	if !test_tcp_write_line(c, fmt.tprintf("delete %d", id)) {
		testing.expectf(t, false, "delete %d: command was not sent", id)
		return
	}
	line, ok := test_tcp_read_line(c)
	testing.expectf(t, ok && line == "DELETED", "delete %d: got %q (ok=%v)", id, line, ok)
}

// queue_e2e_stats_job fetches stats-job and returns the YAML body.
queue_e2e_stats_job :: proc(t: ^testing.T, c: ^Test_Conn, id: u64) -> (yaml: string, ok: bool) {
	if !test_tcp_write_line(c, fmt.tprintf("stats-job %d", id)) {
		testing.expectf(t, false, "stats-job %d: command was not sent", id)
		return "", false
	}
	return test_queue_read_ok_yaml(c)
}

// ---------------------------------------------------------------- tests

@(test)
test_queue_e2e_put_reserve_delete :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	env := queue_test_start()
	if env == nil {
		testing.expectf(t, false, "queue server did not start")
		return
	}
	defer queue_test_stop(env)

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	id, _, put_ok := test_queue_put(&c, 0, 0, 30, "hello")
	if !put_ok {
		testing.expectf(t, false, "put did not return INSERTED")
		return
	}

	got, body, rok := queue_e2e_reserve(&c)
	if !rok {
		testing.expectf(t, false, "reserve did not return RESERVED")
		return
	}
	testing.expect_value(t, got, id)
	testing.expect_value(t, body, "hello")

	queue_e2e_delete(t, &c, id)

	// Deleting an unknown or already-deleted job reports NOT_FOUND.
	if !test_tcp_write_line(&c, fmt.tprintf("delete %d", id)) {
		testing.expectf(t, false, "second delete was not sent")
		return
	}
	line, lok := test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "NOT_FOUND", "second delete: got %q (ok=%v)", line, lok)
}

@(test)
test_queue_e2e_priority :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	env := queue_test_start()
	if env == nil {
		testing.expectf(t, false, "queue server did not start")
		return
	}
	defer queue_test_stop(env)

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	id_low, _, low_ok := test_queue_put(&c, 100, 0, 30, "low")
	id_high, _, high_ok := test_queue_put(&c, 10, 0, 30, "high")
	if !low_ok || !high_ok {
		testing.expectf(t, false, "priority puts failed (low=%v high=%v)", low_ok, high_ok)
		return
	}

	first_id, first_body, first_ok := queue_e2e_reserve(&c)
	if !first_ok {
		testing.expectf(t, false, "first reserve failed")
		return
	}
	testing.expect_value(t, first_id, id_high)
	testing.expect_value(t, first_body, "high")

	second_id, second_body, second_ok := queue_e2e_reserve(&c)
	if !second_ok {
		testing.expectf(t, false, "second reserve failed")
		return
	}
	testing.expect_value(t, second_id, id_low)
	testing.expect_value(t, second_body, "low")

	ids := [2]u64{first_id, second_id}
	for id in ids {
		queue_e2e_delete(t, &c, id)
	}
}

@(test)
test_queue_e2e_delay :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	env := queue_test_start()
	if env == nil {
		testing.expectf(t, false, "queue server did not start")
		return
	}
	defer queue_test_stop(env)

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	id, _, put_ok := test_queue_put(&c, 0, 1, 30, "later")
	if !put_ok {
		testing.expectf(t, false, "put with delay failed")
		return
	}

	yaml, yok := queue_e2e_stats_job(t, &c, id)
	if !yok {
		testing.expectf(t, false, "stats-job failed")
		return
	}
	testing.expectf(t, strings.contains(yaml, "state: delayed"), "stats-job reported %q", yaml)

	// A zero timeout must not see the delayed job yet.
	if !test_tcp_write_line(&c, "reserve-with-timeout 0") {
		testing.expectf(t, false, "reserve was not sent")
		return
	}
	line, lok := test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "TIMED_OUT", "reserve before delay: got %q (ok=%v)", line, lok)

	time.sleep(1200 * time.Millisecond)

	got, body, rok := queue_e2e_reserve(&c, 5)
	if !rok {
		testing.expectf(t, false, "reserve after delay did not return RESERVED")
		return
	}
	testing.expect_value(t, got, id)
	testing.expect_value(t, body, "later")

	queue_e2e_delete(t, &c, id)
}

@(test)
test_queue_e2e_ttr_timeout :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	env := queue_test_start()
	if env == nil {
		testing.expectf(t, false, "queue server did not start")
		return
	}
	defer queue_test_stop(env)

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	id, _, put_ok := test_queue_put(&c, 0, 0, 1, "short ttr")
	if !put_ok {
		testing.expectf(t, false, "put failed")
		return
	}
	first_id, _, first_ok := queue_e2e_reserve(&c)
	if !first_ok {
		testing.expectf(t, false, "first reserve failed")
		return
	}
	testing.expect_value(t, first_id, id)

	// Let TTR expire; the queue worker moves the job back to ready.
	time.sleep(1200 * time.Millisecond)

	again_id, again_body, again_ok := queue_e2e_reserve(&c, 5)
	if !again_ok {
		testing.expectf(t, false, "reserve after TTR did not return RESERVED")
		return
	}
	testing.expect_value(t, again_id, id)
	testing.expect_value(t, again_body, "short ttr")

	yaml, yok := queue_e2e_stats_job(t, &c, id)
	if !yok {
		testing.expectf(t, false, "stats-job failed")
		return
	}
	testing.expectf(t, strings.contains(yaml, "timeouts: 1"), "stats-job reported %q", yaml)
	testing.expectf(t, strings.contains(yaml, "reserves: 2"), "stats-job reported %q", yaml)

	queue_e2e_delete(t, &c, id)
}

@(test)
test_queue_e2e_bury_release_kick :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	env := queue_test_start()
	if env == nil {
		testing.expectf(t, false, "queue server did not start")
		return
	}
	defer queue_test_stop(env)

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	id, _, put_ok := test_queue_put(&c, 10, 0, 30, "job")
	if !put_ok {
		testing.expectf(t, false, "put failed")
		return
	}
	reserved_id, _, rok := queue_e2e_reserve(&c)
	if !rok {
		testing.expectf(t, false, "reserve failed")
		return
	}
	testing.expect_value(t, reserved_id, id)

	if !test_tcp_write_line(&c, fmt.tprintf("bury %d", id)) {
		testing.expectf(t, false, "bury was not sent")
		return
	}
	line, lok := test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "BURIED", "bury: got %q (ok=%v)", line, lok)

	// Buried jobs are invisible to reserve but visible to peek-buried.
	if !test_tcp_write_line(&c, "reserve-with-timeout 0") {
		testing.expectf(t, false, "reserve was not sent")
		return
	}
	line, lok = test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "TIMED_OUT", "reserve while buried: got %q (ok=%v)", line, lok)

	if !test_tcp_write_line(&c, "peek-buried") {
		testing.expectf(t, false, "peek-buried was not sent")
		return
	}
	peeked_id, peeked_body, peek_ok := test_queue_read_job(&c, "FOUND")
	if !peek_ok {
		testing.expectf(t, false, "peek-buried did not return FOUND")
		return
	}
	testing.expect_value(t, peeked_id, id)
	testing.expect_value(t, peeked_body, "job")

	if !test_tcp_write_line(&c, "kick 1") {
		testing.expectf(t, false, "kick was not sent")
		return
	}
	line, lok = test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "KICKED 1", "kick: got %q (ok=%v)", line, lok)

	kicked_id, _, kick_ok := queue_e2e_reserve(&c)
	if !kick_ok {
		testing.expectf(t, false, "reserve after kick did not return RESERVED")
		return
	}
	testing.expect_value(t, kicked_id, id)

	// Releasing with a new priority puts the job back as ready.
	if !test_tcp_write_line(&c, fmt.tprintf("release %d 5 0", id)) {
		testing.expectf(t, false, "release was not sent")
		return
	}
	line, lok = test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "RELEASED", "release: got %q (ok=%v)", line, lok)

	yaml, yok := queue_e2e_stats_job(t, &c, id)
	if !yok {
		testing.expectf(t, false, "stats-job failed")
		return
	}
	testing.expectf(t, strings.contains(yaml, "pri: 5"), "stats-job reported %q", yaml)
	testing.expectf(t, strings.contains(yaml, "state: ready"), "stats-job reported %q", yaml)

	released_id, _, released_ok := queue_e2e_reserve(&c)
	if !released_ok {
		testing.expectf(t, false, "reserve after release failed")
		return
	}
	testing.expect_value(t, released_id, id)

	queue_e2e_delete(t, &c, id)
}

@(test)
test_queue_e2e_pause_tube :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	env := queue_test_start()
	if env == nil {
		testing.expectf(t, false, "queue server did not start")
		return
	}
	defer queue_test_stop(env)

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	if !test_tcp_write_line(&c, "use work") {
		testing.expectf(t, false, "use work was not sent")
		return
	}
	line, lok := test_tcp_read_line(&c)
	if !testing.expectf(t, lok && line == "USING work", "use work: got %q (ok=%v)", line, lok) {
		return
	}

	// reserve only scans watched tubes, so the paused tube must be watched too.
	if !test_tcp_write_line(&c, "watch work") {
		testing.expectf(t, false, "watch work was not sent")
		return
	}
	line, lok = test_tcp_read_line(&c)
	if !testing.expectf(t, lok && line == "WATCHING 2", "watch work: got %q (ok=%v)", line, lok) {
		return
	}

	id, _, put_ok := test_queue_put(&c, 0, 0, 30, "paused job")
	if !put_ok {
		testing.expectf(t, false, "put failed")
		return
	}

	if !test_tcp_write_line(&c, "pause-tube work 1") {
		testing.expectf(t, false, "pause-tube was not sent")
		return
	}
	line, lok = test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "PAUSED", "pause-tube: got %q (ok=%v)", line, lok)

	if !test_tcp_write_line(&c, "reserve-with-timeout 0") {
		testing.expectf(t, false, "reserve was not sent")
		return
	}
	line, lok = test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "TIMED_OUT", "reserve while paused: got %q (ok=%v)", line, lok)

	if !test_tcp_write_line(&c, "stats-tube work") {
		testing.expectf(t, false, "stats-tube was not sent")
		return
	}
	yaml, yok := test_queue_read_ok_yaml(&c)
	if !yok {
		testing.expectf(t, false, "stats-tube did not return OK yaml")
		return
	}
	testing.expectf(t, strings.contains(yaml, "name: work"), "stats-tube reported %q", yaml)
	testing.expectf(t, strings.contains(yaml, "pause: 1"), "stats-tube reported %q", yaml)

	// Unpausing with 0 makes the ready job reservable again.
	if !test_tcp_write_line(&c, "pause-tube work 0") {
		testing.expectf(t, false, "unpause-tube was not sent")
		return
	}
	line, lok = test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "PAUSED", "unpause-tube: got %q (ok=%v)", line, lok)

	got, body, rok := queue_e2e_reserve(&c)
	if !rok {
		testing.expectf(t, false, "reserve after unpause failed")
		return
	}
	testing.expect_value(t, got, id)
	testing.expect_value(t, body, "paused job")

	queue_e2e_delete(t, &c, id)
}

@(test)
test_queue_e2e_use_watch_ignore_stats :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	env := queue_test_start()
	if env == nil {
		testing.expectf(t, false, "queue server did not start")
		return
	}
	defer queue_test_stop(env)

	c, cok := test_tcp_dial(env.addr)
	if !cok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&c)

	if !test_tcp_write_line(&c, "watch work") {
		testing.expectf(t, false, "watch work was not sent")
		return
	}
	line, lok := test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "WATCHING 2", "watch work: got %q (ok=%v)", line, lok)

	if !test_tcp_write_line(&c, "ignore default") {
		testing.expectf(t, false, "ignore default was not sent")
		return
	}
	line, lok = test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "WATCHING 1", "ignore default: got %q (ok=%v)", line, lok)

	if !test_tcp_write_line(&c, "use work") {
		testing.expectf(t, false, "use work was not sent")
		return
	}
	line, lok = test_tcp_read_line(&c)
	testing.expectf(t, lok && line == "USING work", "use work: got %q (ok=%v)", line, lok)

	id, _, put_ok := test_queue_put(&c, 5, 0, 30, "stats job")
	if !put_ok {
		testing.expectf(t, false, "put failed")
		return
	}

	if !test_tcp_write_line(&c, "stats") {
		testing.expectf(t, false, "stats was not sent")
		return
	}
	yaml, yok := test_queue_read_ok_yaml(&c)
	if !yok {
		testing.expectf(t, false, "stats did not return OK yaml")
		return
	}
	testing.expectf(t, strings.contains(yaml, "current-jobs-ready: 1"), "stats reported %q", yaml)
	testing.expectf(t, strings.contains(yaml, "total-jobs: 1"), "stats reported %q", yaml)
	testing.expectf(t, strings.contains(yaml, "current-using: 1"), "stats reported %q", yaml)
	testing.expectf(t, strings.contains(yaml, "current-watching: 1"), "stats reported %q", yaml)

	if !test_tcp_write_line(&c, "stats-tube work") {
		testing.expectf(t, false, "stats-tube was not sent")
		return
	}
	tube_yaml, tube_ok := test_queue_read_ok_yaml(&c)
	if !tube_ok {
		testing.expectf(t, false, "stats-tube did not return OK yaml")
		return
	}
	testing.expectf(t, strings.contains(tube_yaml, "name: work"), "stats-tube reported %q", tube_yaml)
	testing.expectf(t, strings.contains(tube_yaml, "current-jobs-ready: 1"), "stats-tube reported %q", tube_yaml)

	if !test_tcp_write_line(&c, "list-tubes-watched") {
		testing.expectf(t, false, "list-tubes-watched was not sent")
		return
	}
	watched_yaml, watched_ok := test_queue_read_ok_yaml(&c)
	if !watched_ok {
		testing.expectf(t, false, "list-tubes-watched did not return OK yaml")
		return
	}
	testing.expectf(t, strings.contains(watched_yaml, "- work"), "watched tubes reported %q", watched_yaml)
	testing.expectf(t, !strings.contains(watched_yaml, "- default"), "watched tubes still contain default: %q", watched_yaml)

	got, body, rok := queue_e2e_reserve(&c)
	if !rok {
		testing.expectf(t, false, "reserve failed")
		return
	}
	testing.expect_value(t, got, id)
	testing.expect_value(t, body, "stats job")

	queue_e2e_delete(t, &c, id)
}

@(test)
test_queue_e2e_disconnect_returns_reserved :: proc(t: ^testing.T) {
	saved_context := context
	context.allocator = runtime.default_allocator()
	defer context = saved_context

	env := queue_test_start()
	if env == nil {
		testing.expectf(t, false, "queue server did not start")
		return
	}
	defer queue_test_stop(env)

	a, aok := test_tcp_dial(env.addr)
	if !aok {
		testing.expectf(t, false, "dial %s failed", env.addr)
		return
	}

	id, _, put_ok := test_queue_put(&a, 0, 0, 30, "orphan")
	if !put_ok {
		testing.expectf(t, false, "put failed")
		test_tcp_close(&a)
		return
	}
	reserved_id, _, rok := queue_e2e_reserve(&a)
	if !rok {
		testing.expectf(t, false, "reserve failed")
		test_tcp_close(&a)
		return
	}
	testing.expect_value(t, reserved_id, id)

	// Closing the reserving connection must return the job to ready.
	test_tcp_close(&a)
	if !queue_test_wait_ready(env.q, 1, 0) {
		testing.expectf(t, false, "job was not returned to ready after disconnect")
		return
	}

	b, bok := test_tcp_dial(env.addr)
	if !bok {
		testing.expectf(t, false, "second dial %s failed", env.addr)
		return
	}
	defer test_tcp_close(&b)

	recovered_id, recovered_body, recovered_ok := queue_e2e_reserve(&b, 5)
	if !recovered_ok {
		testing.expectf(t, false, "reserve after disconnect did not return RESERVED")
		return
	}
	testing.expect_value(t, recovered_id, id)
	testing.expect_value(t, recovered_body, "orphan")

	queue_e2e_delete(t, &b, id)
}
