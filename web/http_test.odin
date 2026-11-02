package web

import "core:net"
import "core:testing"

@(test)
test_server_destroy_without_run :: proc(t: ^testing.T) {
	// server_destroy must not close stdin or join missing workers when the
	// server was initialized but never started.
	srv := server_init(default_server_config(), nil)
	testing.expect(t, srv != nil)
	server_destroy(srv)

	// A stop that never had a listener must not close fd 0, and the following
	// destroy has to tolerate the already-stopped state.
	srv = server_init(default_server_config(), nil)
	server_stop(srv)
	testing.expect_value(t, srv.listener, net.TCP_Socket(0))
	server_destroy(srv)
}

@(test)
test_status_and_crlf_helpers :: proc(t: ^testing.T) {
	testing.expect_value(t, status_text(200), "OK")
	testing.expect_value(t, status_text(204), "No Content")
	testing.expect_value(t, status_text(404), "Not Found")
	testing.expect_value(t, status_text(500), "Internal Server Error")
	testing.expect_value(t, status_text(999), "Unknown")

	pos := 0
	line, ok := next_crlf("GET / HTTP/1.1\r\nHost: example\r\n", &pos)
	testing.expect(t, ok)
	testing.expect_value(t, line, "GET / HTTP/1.1")
	testing.expect_value(t, pos, 16)
	line, ok = next_crlf("GET / HTTP/1.1\r\nHost: example\r\n", &pos)
	testing.expect(t, ok)
	testing.expect_value(t, line, "Host: example")
	_, ok = next_crlf("GET / HTTP/1.1\r\nHost: example\r\n", &pos)
	testing.expect(t, !ok)
}
