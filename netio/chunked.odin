// chunked.odin: the single incremental decoder for HTTP/1.1 chunked framing.
//
// Both halves of the HTTP server call this: the event loop uses it to decide
// when a request body is complete (without decoding), and the request parser
// uses it to extract the payload. Keeping one state machine guarantees that
// "needs more data", "too large" and "bad format" are classified identically
// on both sides.
package netio

import "../common"
import "core:strconv"
import "core:strings"

Chunk_Result :: enum {
	Need_More,
	Complete,
	// Bad_Format covers an invalid chunk size, a broken CRLF after chunk data
	// and an over-long framing line. HTTP maps it to 400.
	Bad_Format,
	// Too_Large means decoded payload would exceed the caller's max_body.
	// HTTP maps it to 413.
	Too_Large,
}

Chunk_Phase :: enum {
	Size,
	Data,
	Crlf,
	Trailer,
}

Chunk_Framer :: struct {
	pos:       int, // next byte to consume, relative to the scanned slice
	phase:     Chunk_Phase,
	remaining: int, // payload bytes left in the current chunk
	decoded:   int, // payload bytes accepted so far
	// end is the offset one past the terminating empty line once Complete.
	end: int,
}

// chunk_frame advances the framer over `data`, which starts right after the
// request head. `max_body` limits the decoded payload; `max_line` limits one
// chunk-size or trailer line. When `out` is non-nil, accepted payload bytes are
// appended to it, which lets the parser decode in the same pass that frames.
// The caller may call it repeatedly with a growing `data` slice; state in f
// keeps the scan incremental.
chunk_frame :: proc(f: ^Chunk_Framer, data: []u8, max_body, max_line: int, out: ^common.Buf = nil) -> Chunk_Result {
	for {
		switch f.phase {
		case .Size:
			rel := f.pos
			idx := strings.index(string(data[rel:]), "\r\n")
			if idx < 0 {
				if len(data) - rel > max_line {
					return .Bad_Format
				}
				return .Need_More
			}
			if idx > max_line {
				return .Bad_Format
			}
			size_str := string(data[rel:rel + idx])
			// strip chunk extensions
			if semi := strings.index_byte(size_str, ';'); semi >= 0 {
				size_str = size_str[:semi]
			}
			size_str = strings.trim_space(size_str)
			if len(size_str) == 0 {
				return .Bad_Format
			}
			size, ok := strconv.parse_int(size_str, 16)
			if !ok || size < 0 {
				return .Bad_Format
			}
			f.pos = rel + idx + 2
			if size == 0 {
				f.phase = .Trailer
				continue
			}
			// Decide from the declared size before consuming any payload so a
			// 413 never depends on how much happened to arrive.
			if size > max_body - f.decoded {
				return .Too_Large
			}
			f.remaining = size
			f.phase = .Data
		case .Data:
			avail := len(data) - f.pos
			if avail == 0 {
				return .Need_More
			}
			take := min(avail, f.remaining)
			if out != nil {
				common.buf_write(out, data[f.pos:f.pos + take])
			}
			f.pos += take
			f.remaining -= take
			f.decoded += take
			if f.remaining > 0 {
				return .Need_More
			}
			f.phase = .Crlf
		case .Crlf:
			if len(data) - f.pos < 2 {
				return .Need_More
			}
			if data[f.pos] != '\r' || data[f.pos + 1] != '\n' {
				return .Bad_Format
			}
			f.pos += 2
			f.phase = .Size
		case .Trailer:
			rel := f.pos
			idx := strings.index(string(data[rel:]), "\r\n")
			if idx < 0 {
				if len(data) - rel > max_line {
					return .Bad_Format
				}
				return .Need_More
			}
			if idx > max_line {
				return .Bad_Format
			}
			f.pos = rel + idx + 2
			if idx == 0 {
				f.end = f.pos
				return .Complete
			}
		}
	}
}
