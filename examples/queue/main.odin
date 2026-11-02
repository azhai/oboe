// Beanstalk-style queue: put, reserve, delete, plus disconnect recovery. The
// in-process API is used directly; no TCP listener is opened.
//
// Run with:
//
//	odin run examples/queue
package main

import "core:fmt"
import "core:time"

import "../../common"
import queuelib "../../queue"

main :: proc() {
	q := queuelib.queue_init(queuelib.default_queue_config())
	defer queuelib.queue_destroy(q)

	conn := queuelib.queue_new_conn(q)

	// pri 0 is the highest priority, delay 0 means ready now, TTR 30s.
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

	if reserved, ok := queuelib.queue_reserve_into(q, []string{queuelib.QUEUE_DEFAULT_TUBE}, conn, 0, &body); ok {
		fmt.printfln("queue: reserved id=%d body=%s", reserved, body.data[:])
		if queuelib.queue_delete(q, reserved, conn) {
			fmt.printfln("queue: deleted id=%d", reserved)
		}
	}

	// Reserve a second job, then simulate the worker losing its connection:
	// queue_conn_closed puts every reserved id back into READY.
	id2, _ := queuelib.queue_put(
		q,
		queuelib.QUEUE_DEFAULT_TUBE,
		0,
		0,
		i64(30 * time.Second),
		transmute([]u8)string("second job"),
	)
	reserved2, rok := queuelib.queue_reserve_into(q, []string{queuelib.QUEUE_DEFAULT_TUBE}, conn, 0, &body)
	if rok {
		fmt.printfln("queue: reserved id=%d before disconnect", reserved2)
		queuelib.queue_conn_closed(q, queuelib.QUEUE_DEFAULT_TUBE, []string{queuelib.QUEUE_DEFAULT_TUBE}, []u64{reserved2})

		common.buf_reset(&body)
		if again, aok := queuelib.queue_reserve_into(q, []string{queuelib.QUEUE_DEFAULT_TUBE}, conn, 0, &body); aok {
			fmt.printfln("queue: reclaimed id=%d body=%s", again, body.data[:])
			queuelib.queue_delete(q, again, conn)
		} else {
			fmt.eprintln("queue: job was not reclaimed")
		}
	} else {
		fmt.eprintln("queue: second reserve failed")
	}
	_ = id2

	st := queuelib.queue_stats(q)
	fmt.printfln(
		"queue: jobs=%d ready=%d reserved=%d delayed=%d buried=%d",
		st.current_jobs,
		st.ready,
		st.reserved,
		st.delayed,
		st.buried,
	)
}
