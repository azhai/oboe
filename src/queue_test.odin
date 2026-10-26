package odlog

import "core:testing"
import "core:time"

queue_test_init :: proc(max_jobs := 0, max_job_bytes := 0) -> ^Queue {
	cfg := default_queue_config()
	if max_jobs > 0 {
		cfg.max_jobs = max_jobs
	}
	if max_job_bytes > 0 {
		cfg.max_job_bytes = max_job_bytes
	}
	return queue_init(cfg)
}

@(test)
test_queue_priority_reserve_delete :: proc(t: ^testing.T) {
	q := queue_test_init()
	defer queue_destroy(q)

	low := transmute([]u8)string("low priority")
	high := transmute([]u8)string("high priority")
	id_low, r1 := queue_put(q, QUEUE_DEFAULT_TUBE, 100, 0, i64(30*time.Second), low)
	testing.expect_value(t, r1, Queue_Put_Result.OK)
	id_high, r2 := queue_put(q, QUEUE_DEFAULT_TUBE, 10, 0, i64(30*time.Second), high)
	testing.expect_value(t, r2, Queue_Put_Result.OK)
	testing.expect(t, id_high > id_low)

	body := buf_init(64)
	defer buf_destroy(&body)

	id, ok := queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 1, 0, &body)
	testing.expect(t, ok)
	testing.expect_value(t, id, id_high)
	testing.expect_value(t, string(body.data[:]), "high priority")

	st := queue_stats(q)
	testing.expect_value(t, st.current_jobs, 2)
	testing.expect_value(t, st.ready, 1)
	testing.expect_value(t, st.reserved, 1)

	testing.expect(t, queue_delete(q, id, 1))
	testing.expect(t, !queue_delete(q, id, 1))
	id, ok = queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 1, 0, &body)
	testing.expect(t, ok)
	testing.expect_value(t, id, id_low)
	testing.expect(t, queue_delete(q, id, 1))

	st = queue_stats(q)
	testing.expect_value(t, st.current_jobs, 0)
	testing.expect_value(t, st.ready, 0)
	testing.expect_value(t, st.reserved, 0)
}

@(test)
test_queue_delay :: proc(t: ^testing.T) {
	q := queue_test_init()
	defer queue_destroy(q)

	body := buf_init(64)
	defer buf_destroy(&body)

	id, _ := queue_put(q, QUEUE_DEFAULT_TUBE, 0, i64(50*time.Millisecond), i64(time.Second), transmute([]u8)string("later"))
	_, ok := queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 1, 0, &body)
	testing.expect(t, !ok)
	testing.expect_value(t, queue_stats(q).delayed, 1)

	time.sleep(80 * time.Millisecond)
	got, got_ok := queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 1, 0, &body)
	testing.expect(t, got_ok)
	testing.expect_value(t, got, id)
	testing.expect_value(t, queue_stats(q).delayed, 0)
	testing.expect(t, queue_delete(q, got, 1))
}

@(test)
test_queue_ttr_timeout :: proc(t: ^testing.T) {
	q := queue_test_init()
	defer queue_destroy(q)

	body := buf_init(64)
	defer buf_destroy(&body)

	id, _ := queue_put(q, QUEUE_DEFAULT_TUBE, 0, 0, i64(20*time.Millisecond), transmute([]u8)string("short ttr"))
	got, ok := queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 1, 0, &body)
	testing.expect(t, ok)
	testing.expect_value(t, got, id)

	time.sleep(80 * time.Millisecond)
	got, ok = queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 2, 0, &body)
	testing.expect(t, ok)
	testing.expect_value(t, got, id)

	st, found := queue_job_stats(q, id)
	testing.expect(t, found)
	testing.expect_value(t, st.timeouts, u64(1))
	testing.expect_value(t, st.state, Job_State.RESERVED)
	testing.expect(t, queue_delete(q, id, 2))
}

@(test)
test_queue_bury_release_kick :: proc(t: ^testing.T) {
	q := queue_test_init()
	defer queue_destroy(q)

	body := buf_init(64)
	defer buf_destroy(&body)

	id, _ := queue_put(q, QUEUE_DEFAULT_TUBE, 50, 0, i64(time.Minute), transmute([]u8)string("job"))
	got, ok := queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 1, 0, &body)
	testing.expect(t, ok)

	testing.expect(t, queue_release(q, got, 1, 5, 0))
	testing.expect(t, !queue_delete(q, got, 1))
	got, ok = queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 1, 0, &body)
	testing.expect(t, ok)
	testing.expect_value(t, got, id)

	testing.expect(t, queue_bury(q, got, 1, 5, true))
	_, ok = queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 1, 0, &body)
	testing.expect(t, !ok)
	testing.expect_value(t, queue_stats(q).buried, 1)

	peeked, pok := queue_peek_into(q, QUEUE_DEFAULT_TUBE, .BURIED, 0, &body)
	testing.expect(t, pok)
	testing.expect_value(t, peeked, id)

	testing.expect(t, queue_kick_job(q, id))
	got, ok = queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 1, 0, &body)
	testing.expect(t, ok)
	testing.expect_value(t, got, id)
	testing.expect(t, queue_delete(q, id, 1))
}

@(test)
test_queue_kick_and_pause :: proc(t: ^testing.T) {
	q := queue_test_init()
	defer queue_destroy(q)

	body := buf_init(64)
	defer buf_destroy(&body)

	id1, _ := queue_put(q, "work", 0, 0, i64(time.Minute), transmute([]u8)string("a"))
	id2, _ := queue_put(q, "work", 0, 0, i64(time.Minute), transmute([]u8)string("b"))
	_, ok := queue_reserve_into(q, []string{"work"}, 1, 0, &body)
	testing.expect(t, ok)
	testing.expect(t, queue_bury(q, id1, 1, 0, false))
	_, ok = queue_reserve_into(q, []string{"work"}, 1, 0, &body)
	testing.expect(t, ok)
	testing.expect(t, queue_bury(q, id2, 1, 0, false))
	testing.expect_value(t, queue_kick(q, "work", 10), 2)
	testing.expect_value(t, queue_stats(q).ready, 2)

	testing.expect(t, queue_pause_tube(q, "work", 5))
	_, ok = queue_reserve_into(q, []string{"work"}, 1, 0, &body)
	testing.expect(t, !ok)
	testing.expect(t, queue_pause_tube(q, "work", 0))
	_, ok = queue_reserve_into(q, []string{"work"}, 1, 0, &body)
	testing.expect(t, ok)
}

@(test)
test_queue_limits_and_disconnect :: proc(t: ^testing.T) {
	q := queue_test_init(max_jobs = 2, max_job_bytes = 4)
	defer queue_destroy(q)

	body := buf_init(64)
	defer buf_destroy(&body)

	_, r := queue_put(q, QUEUE_DEFAULT_TUBE, 0, 0, i64(time.Minute), transmute([]u8)string("12345"))
	testing.expect_value(t, r, Queue_Put_Result.TOO_BIG)

	id1, r1 := queue_put(q, QUEUE_DEFAULT_TUBE, 0, 0, i64(time.Minute), transmute([]u8)string("aa"))
	testing.expect_value(t, r1, Queue_Put_Result.OK)
	_, r2 := queue_put(q, QUEUE_DEFAULT_TUBE, 0, 0, i64(time.Minute), transmute([]u8)string("bb"))
	testing.expect_value(t, r2, Queue_Put_Result.OK)
	_, r3 := queue_put(q, QUEUE_DEFAULT_TUBE, 0, 0, i64(time.Minute), transmute([]u8)string("cc"))
	testing.expect_value(t, r3, Queue_Put_Result.DRAINING)

	got, ok := queue_reserve_into(q, []string{QUEUE_DEFAULT_TUBE}, 7, 0, &body)
	testing.expect(t, ok)
	testing.expect_value(t, got, id1)
	queue_conn_closed(q, "", nil, []u64{got})
	testing.expect_value(t, queue_stats(q).ready, 2)
	testing.expect_value(t, queue_stats(q).reserved, 0)
}
