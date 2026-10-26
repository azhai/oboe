package odlog

import "core:fmt"
import "core:strconv"
import "core:strings"

Filter_Kind :: enum {
	AND,
	OR,
	NOT,
	WORD,
	PHRASE,
	EXACT,
	PREFIX,
	EXISTS,
	RANGE,
	TIME_RANGE,
	ALL,
}

Compare_Op :: enum {
	EQ,
	GT,
	GTE,
	LT,
	LTE,
}

Filter :: struct {
	kind:      Filter_Kind,
	kids:      [dynamic]^Filter,
	field:     string,
	value:     string,
	cmp:       Compare_Op,
	ts_start:  i64,
	ts_end:    i64,
	has_start: bool,
	has_end:   bool,
}

Pipe_Kind :: enum {
	LIMIT,
	SORT,
	FIELDS,
	STATS,
}

Sort_Spec :: struct {
	name: string,
	desc: bool,
}

Agg_Kind :: enum {
	COUNT,
	SUM,
	AVG,
	MIN,
	MAX,
}

Agg_Spec :: struct {
	kind:  Agg_Kind,
	field: string,
	out:   string,
}

Pipe :: struct {
	kind:   Pipe_Kind,
	limit:  int,
	sort:   []Sort_Spec,
	fields: []string,
	by:     []string,
	aggs:   []Agg_Spec,
}

Query :: struct {
	filter:    ^Filter,
	pipes:     []Pipe,
	start:     i64,
	end:       i64,
	has_start: bool,
	has_end:   bool,
	now:       i64,
	arena:     Arena,
}

// ---------------------------------------------------------------- lexer

Tok_Kind :: enum {
	WORD,
	STRING,
	COLON,
	EQUAL,
	LPAREN,
	RPAREN,
	LBRACKET,
	RBRACKET,
	COMMA,
	PIPE,
	STAR,
	GT,
	GTE,
	LT,
	LTE,
	EOF,
}

Token :: struct {
	kind: Tok_Kind,
	text: string,
}

is_query_word_byte :: #force_inline proc(b: u8) -> bool {
	switch b {
	case ' ', '\t', '\r', '\n', '(', ')', '[', ']', '{', '}', ',', '"', '|', ':', '=', '<', '>', '~', '*':
		return false
	}
	return true
}

// allows_iso_colon reports whether the word prefix looks like a date/time, so
// that `_time:2026-10-26T10:00:00Z` stays one token instead of splitting at ':'.
allows_iso_colon :: #force_inline proc(prefix: string) -> bool {
	if len(prefix) == 0 || prefix[0] < '0' || prefix[0] > '9' {
		return false
	}
	for c in prefix {
		if c == '-' || c == 'T' || c == 't' {
			return true
		}
	}
	return false
}

lex_query :: proc(s: string, arena: ^Arena) -> ([dynamic]Token, bool) {
	toks: [dynamic]Token
	i := 0
	for i < len(s) {
		c := s[i]
		switch c {
		case ' ', '\t', '\r', '\n':
			i += 1
		case '(':
			append(&toks, Token{.LPAREN, "("})
			i += 1
		case ')':
			append(&toks, Token{.RPAREN, ")"})
			i += 1
		case '[':
			append(&toks, Token{.LBRACKET, "["})
			i += 1
		case ']':
			append(&toks, Token{.RBRACKET, "]"})
			i += 1
		case ',':
			append(&toks, Token{.COMMA, ","})
			i += 1
		case '|':
			append(&toks, Token{.PIPE, "|"})
			i += 1
		case ':':
			append(&toks, Token{.COLON, ":"})
			i += 1
		case '*':
			append(&toks, Token{.STAR, "*"})
			i += 1
		case '=':
			append(&toks, Token{.EQUAL, "="})
			i += 1
		case '<':
			if i + 1 < len(s) && s[i+1] == '=' {
				append(&toks, Token{.LTE, "<="})
				i += 2
			} else {
				append(&toks, Token{.LT, "<"})
				i += 1
			}
		case '>':
			if i + 1 < len(s) && s[i+1] == '=' {
				append(&toks, Token{.GTE, ">="})
				i += 2
			} else {
				append(&toks, Token{.GT, ">"})
				i += 1
			}
		case '"':
			str, ok := parse_json_string(s, &i, arena)
			if !ok {
				delete(toks)
				return nil, false
			}
			append(&toks, Token{.STRING, str})
		case:
			start := i
			for i < len(s) {
				c := s[i]
				if c == ':' {
					if !allows_iso_colon(s[start:i]) {
						break
					}
					i += 1
					continue
				}
				if !is_query_word_byte(c) {
					break
				}
				i += 1
			}
			if i == start {
				delete(toks)
				return nil, false
			}
			append(&toks, Token{.WORD, s[start:i]})
		}
	}
	append(&toks, Token{.EOF, ""})
	return toks, true
}

// ---------------------------------------------------------------- parser

Parser :: struct {
	toks:  []Token,
	pos:   int,
	arena: ^Arena,
	now:   i64,
}

peek :: #force_inline proc(p: ^Parser) -> Token {
	return p.toks[p.pos]
}

at_end :: #force_inline proc(p: ^Parser) -> bool {
	return p.toks[p.pos].kind == .EOF
}

advance :: #force_inline proc(p: ^Parser) -> Token {
	t := p.toks[p.pos]
	if t.kind != .EOF {
		p.pos += 1
	}
	return t
}

accept :: #force_inline proc(p: ^Parser, k: Tok_Kind) -> bool {
	if p.toks[p.pos].kind == k {
		p.pos += 1
		return true
	}
	return false
}

accept_word :: proc(p: ^Parser, w: string) -> bool {
	t := p.toks[p.pos]
	if t.kind == .WORD && token_eq_ci(t.text, w) {
		p.pos += 1
		return true
	}
	return false
}

new_filter :: proc(kind: Filter_Kind) -> ^Filter {
	f := new(Filter)
	f.kind = kind
	return f
}

add_kid :: proc(f: ^Filter, kid: ^Filter) {
	append(&f.kids, kid)
}

parse_query_string :: proc(qstr: string, now: i64) -> (q: Query, ok: bool) {
	q.now = now
	q.end = now + DAY_NS
	toks, lok := lex_query(qstr, &q.arena)
	if !lok {
		return q, false
	}
	defer delete(toks)
	p := Parser{toks = toks[:], arena = &q.arena, now = now}
	if !at_end(&p) && peek(&p).kind != .PIPE {
		q.filter, ok = parse_or(&p)
		if !ok {
			query_destroy(&q)
			return q, false
		}
	}
	pipes: [dynamic]Pipe
	for accept(&p, .PIPE) {
		pipe, pok := parse_pipe(&p)
		if !pok {
			for pp in pipes {
				free_pipe(pp)
			}
			delete(pipes)
			query_destroy(&q)
			return q, false
		}
		append(&pipes, pipe)
	}
	if !at_end(&p) {
		for pp in pipes {
			free_pipe(pp)
		}
		delete(pipes)
		query_destroy(&q)
		return q, false
	}
	q.pipes = pipes[:]
	if q.filter != nil {
		apply_filter_time(&q, q.filter)
	}
	return q, true
}

apply_filter_time :: proc(q: ^Query, f: ^Filter) {
	#partial switch f.kind {
	case .TIME_RANGE:
		if f.has_start {

			if !q.has_start || f.ts_start > q.start {
				q.start = f.ts_start
			}
			q.has_start = true
		}
		if f.has_end {
			if !q.has_end || f.ts_end < q.end {
				q.end = f.ts_end
			}
			q.has_end = true
		}
	case .AND:
		for k in f.kids {
			apply_filter_time(q, k)
		}
	case .OR:
		for k in f.kids {
			apply_filter_time(q, k)
		}
	case:
	}
}

parse_or :: proc(p: ^Parser) -> (^Filter, bool) {
	left, ok := parse_and(p)
	if !ok {
		return nil, false
	}
	if !(peek(p).kind == .WORD && token_eq_ci(peek(p).text, "or")) {
		return left, true
	}
	or := new_filter(.OR)
	add_kid(or, left)
	for accept_word(p, "or") {
		right, rok := parse_and(p)
		if !rok {
			free_filter(or)
			return nil, false
		}
		add_kid(or, right)
	}
	return or, true
}

starts_filter :: proc(p: ^Parser) -> bool {
	t := peek(p)
	if t.kind == .WORD && (token_eq_ci(t.text, "or") || token_eq_ci(t.text, "and")) {
		return false
	}
	#partial switch t.kind {
	case .WORD, .STRING, .LPAREN, .STAR:
		return true
	}
	return false
}

parse_and :: proc(p: ^Parser) -> (^Filter, bool) {
	left, ok := parse_unary(p)
	if !ok {
		return nil, false
	}
	and := new_filter(.AND)
	add_kid(and, left)
	for {
		if accept_word(p, "and") {
			continue
		}
		if !starts_filter(p) {
			break
		}
		right, rok := parse_unary(p)
		if !rok {
			free_filter(and)
			return nil, false
		}
		add_kid(and, right)
	}
	if len(and.kids) == 1 {
		only := and.kids[0]
		delete(and.kids)
		free(and)
		return only, true
	}
	return and, true
}

parse_unary :: proc(p: ^Parser) -> (^Filter, bool) {
	t := peek(p)
	if t.kind == .WORD && token_eq_ci(t.text, "not") {
		advance(p)
		kid, ok := parse_unary(p)
		if !ok {
			return nil, false
		}
		n := new_filter(.NOT)
		add_kid(n, kid)
		return n, true
	}
	if t.kind == .WORD && len(t.text) > 1 && t.text[0] == '-' {
		advance(p)
		synth := Token{kind = .WORD, text = t.text[1:]}
		kid, ok := parse_primary(p, synth)
		if !ok {
			return nil, false
		}
		n := new_filter(.NOT)
		add_kid(n, kid)
		return n, true
	}
	if accept(p, .LPAREN) {
		inner, ok := parse_or(p)
		if !ok {
			return nil, false
		}
		if !accept(p, .RPAREN) {
			free_filter(inner)
			return nil, false
		}
		return inner, true
	}
	if accept(p, .STAR) {
		return new_filter(.ALL), true
	}
	if t.kind != .WORD && t.kind != .STRING {
		return nil, false
	}
	advance(p)
	return parse_primary(p, t)
}

parse_primary :: proc(p: ^Parser, t: Token) -> (^Filter, bool) {
	if t.kind == .WORD && token_eq_ci(t.text, "in") && peek(p).kind == .LPAREN {
		advance(p)
		field := "_msg"
		or := new_filter(.OR)
		for {
			vt := advance(p)
			if vt.kind != .WORD && vt.kind != .STRING {
				free_filter(or)
				return nil, false
			}
			f := new_filter(.WORD)
			f.field = field
			f.value = vt.text
			add_kid(or, f)
			if accept(p, .COMMA) {
				continue
			}
			if accept_word(p, "or") {
				continue
			}
			break
		}
		if !accept(p, .RPAREN) {
			free_filter(or)
			return nil, false
		}
		return or, true
	}
	if peek(p).kind != .COLON {
		field := "_msg"
		kind: Filter_Kind = .WORD
		if t.kind == .STRING {
			kind = .PHRASE
		}
		f := new_filter(kind)
		f.field = field
		f.value = t.text
		if accept(p, .STAR) {
			f.kind = .PREFIX
		}
		return f, true
	}
	advance(p) // colon
	field := t.text
	if field == "_time" {
		return parse_time_filter(p)
	}
	eq := accept(p, .EQUAL)
	cmp := Compare_Op.EQ
	if accept(p, .GT) {
		cmp = .GT
	} else if accept(p, .GTE) {
		cmp = .GTE
	} else if accept(p, .LT) {
		cmp = .LT
	} else if accept(p, .LTE) {
		cmp = .LTE
	}
	if accept(p, .STAR) {
		f := new_filter(.EXISTS)
		f.field = field
		return f, true
	}
	if peek(p).kind == .LPAREN {
		advance(p)
		or := new_filter(.OR)
		for {
			vt := advance(p)
			if vt.kind != .WORD && vt.kind != .STRING {
				free_filter(or)
				return nil, false
			}
			f := new_filter(.WORD)
			f.field = field
			f.value = vt.text
			add_kid(or, f)
			if accept(p, .COMMA) {
				continue
			}
			if accept_word(p, "or") {
				continue
			}
			break
		}
		if !accept(p, .RPAREN) {
			free_filter(or)
			return nil, false
		}
		return or, true
	}
	vt := advance(p)
	if vt.kind != .WORD && vt.kind != .STRING {
		return nil, false
	}
	if cmp != .EQ {
		f := new_filter(.RANGE)
		f.field = field
		f.value = vt.text
		f.cmp = cmp
		return f, true
	}
	f := new_filter(.WORD)
	f.field = field
	f.value = vt.text
	if eq {
		f.kind = .EXACT
	} else if vt.kind == .STRING {
		f.kind = .PHRASE
	}
	if accept(p, .STAR) {
		f.kind = .PREFIX
	}
	return f, true
}

parse_time_filter :: proc(p: ^Parser) -> (^Filter, bool) {
	f := new_filter(.TIME_RANGE)
	if accept(p, .LBRACKET) {
		for i in 0..<2 {
			t := advance(p)
			if t.kind == .STAR {
				if i == 0 {
					f.ts_start = 0
					f.has_start = true
				} else {
					f.ts_end = i64(0x7fffffffffffffff)
					f.has_end = true
				}
			} else if t.kind == .WORD || t.kind == .STRING {
				ts: i64
				ok: bool
				ts, ok = parse_time_ns(t.text, p.now)
				if !ok {
					ts, ok = parse_duration_ns(t.text)
					if ok {
						ts = p.now - ts
					}
				}
				if !ok {
					free_filter(f)
					return nil, false
				}
				if i == 0 {
					f.ts_start = ts
					f.has_start = true
				} else {
					f.ts_end = ts
					f.has_end = true
				}
			} else {
				free_filter(f)
				return nil, false
			}
			if i == 0 && !accept(p, .COMMA) {
				free_filter(f)
				return nil, false
			}
		}
		if !accept(p, .RBRACKET) {
			free_filter(f)
			return nil, false
		}
		return f, true
	}
	cmp := Compare_Op.EQ
	if accept(p, .GT) {
		cmp = .GT
	} else if accept(p, .GTE) {
		cmp = .GTE
	} else if accept(p, .LT) {
		cmp = .LT
	} else if accept(p, .LTE) {
		cmp = .LTE
	}
	t := advance(p)
	if t.kind != .WORD && t.kind != .STRING {
		free_filter(f)
		return nil, false
	}
	ts, ok := parse_time_ns(t.text, p.now)
	if ok {
		f.ts_start = ts
		f.ts_end = ts
		switch cmp {
		case .GT:
			f.ts_start = ts + 1
			f.has_start = true
		case .GTE, .EQ:
			f.has_start = true
			f.has_end = true
		case .LT:
			f.ts_end = ts - 1
			f.has_end = true
		case .LTE:
			f.has_end = true
		}
		return f, true
	}
	if d, dok := parse_duration_ns(t.text); dok {
		f.ts_start = p.now - d
		f.ts_end = p.now
		f.has_start = true
		f.has_end = true
		return f, true
	}
	free_filter(f)
	return nil, false
}

// ---------------------------------------------------------------- pipes

parse_pipe :: proc(p: ^Parser) -> (Pipe, bool) {
	t := advance(p)
	if t.kind != .WORD {
		return {}, false
	}
	if token_eq_ci(t.text, "limit") {
		n := advance(p)
		if n.kind != .WORD {
			return {}, false
		}
		v, ok := strconv.parse_int(n.text, 10)
		if !ok || v < 0 {
			return {}, false
		}
		return Pipe{kind = .LIMIT, limit = v}, true
	}
	if token_eq_ci(t.text, "sort") {
		accept_word(p, "by")
		accept(p, .LPAREN)
		specs: [dynamic]Sort_Spec
		for {
			st := advance(p)
			if st.kind != .WORD && st.kind != .STRING {
				delete(specs)
				return {}, false
			}
			spec := Sort_Spec{name = st.text}
			if peek(p).kind == .WORD && token_eq_ci(peek(p).text, "desc") {
				advance(p)
				spec.desc = true
			} else if peek(p).kind == .WORD && token_eq_ci(peek(p).text, "asc") {
				advance(p)
			}
			append(&specs, spec)
			if accept(p, .COMMA) {
				continue
			}
			break
		}
		accept(p, .RPAREN)
		if peek(p).kind == .WORD && token_eq_ci(peek(p).text, "desc") {
			advance(p)
			for &spec in specs {
				spec.desc = true
			}
		} else if peek(p).kind == .WORD && token_eq_ci(peek(p).text, "asc") {
			advance(p)
			for &spec in specs {
				spec.desc = false
			}
		}
		return Pipe{kind = .SORT, sort = specs[:]}, true
	}
	if token_eq_ci(t.text, "fields") || token_eq_ci(t.text, "keep") {
		fields: [dynamic]string
		for {
			ft := advance(p)
			if ft.kind != .WORD && ft.kind != .STRING {
				delete(fields)
				return {}, false
			}
			append(&fields, ft.text)
			if accept(p, .COMMA) {
				continue
			}
			break
		}
		return Pipe{kind = .FIELDS, fields = fields[:]}, true
	}
	if token_eq_ci(t.text, "stats") {
		pipe := Pipe{kind = .STATS}
		if accept_word(p, "by") {
			if !accept(p, .LPAREN) {
				return {}, false
			}
			by: [dynamic]string
			for {
				bt := advance(p)
				if bt.kind != .WORD && bt.kind != .STRING {
					delete(by)
					return {}, false
				}
				append(&by, bt.text)
				if accept(p, .COMMA) {
					continue
				}
				break
			}
			if !accept(p, .RPAREN) {
				delete(by)
				return {}, false
			}
			pipe.by = by[:]
		}
		aggs: [dynamic]Agg_Spec
		for {
			at := advance(p)
			if at.kind != .WORD {
				delete(aggs)
				return {}, false
			}
			agg: Agg_Spec
			switch {
			case token_eq_ci(at.text, "count"):
				agg.kind = .COUNT
				agg.out = "count"
			case token_eq_ci(at.text, "sum"):
				agg.kind = .SUM
				agg.out = "sum"
			case token_eq_ci(at.text, "avg"):
				agg.kind = .AVG
				agg.out = "avg"
			case token_eq_ci(at.text, "min"):
				agg.kind = .MIN
				agg.out = "min"
			case token_eq_ci(at.text, "max"):
				agg.kind = .MAX
				agg.out = "max"
			case:
				delete(aggs)
				return {}, false
			}
			if accept(p, .LPAREN) {
				if peek(p).kind == .WORD || peek(p).kind == .STRING {
					ft := advance(p)
					agg.field = ft.text
				}
				if !accept(p, .RPAREN) {
					delete(aggs)
					return {}, false
				}
			}
			if accept_word(p, "as") {
				jt := advance(p)
				if jt.kind != .WORD && jt.kind != .STRING {
					delete(aggs)
					return {}, false
				}
				agg.out = jt.text
			}
			append(&aggs, agg)
			if accept(p, .COMMA) {
				continue
			}
			break
		}
		pipe.aggs = aggs[:]
		return pipe, true
	}
	return {}, false
}

// ---------------------------------------------------------------- cleanup

free_pipe :: proc(p: Pipe) {
	delete(p.sort)
	delete(p.fields)
	delete(p.by)
	delete(p.aggs)
}

free_filter :: proc(f: ^Filter) {
	if f == nil {
		return
	}
	for k in f.kids {
		free_filter(k)
	}
	delete(f.kids)
	free(f)
}

query_destroy :: proc(q: ^Query) {
	if q.filter != nil {
		free_filter(q.filter)
	}
	for p in q.pipes {
		free_pipe(p)
	}
	delete(q.pipes)
	arena_destroy(&q.arena)
	q^ = {}
}

// ---------------------------------------------------------------- helpers

apply_query_bounds :: proc(q: ^Query, start_param, end_param: string) -> bool {
	if len(start_param) > 0 {
		ts, ok := parse_time_ns(start_param, q.now)
		if !ok {
			return false
		}
		if !q.has_start || ts > q.start {
			q.start = ts
		}
		q.has_start = true
	}
	if len(end_param) > 0 {
		ts, ok := parse_time_ns(end_param, q.now)
		if !ok {
			return false
		}
		if !q.has_end || ts < q.end {
			q.end = ts
		}
		q.has_end = true
	}
	return true
}

query_limit :: proc(q: ^Query, default_limit: int) -> (limit: int, has_limit: bool) {
	for p in q.pipes {
		if p.kind == .LIMIT {
			return p.limit, true
		}
	}
	return default_limit, false
}

query_has_pipe :: proc(q: ^Query, kind: Pipe_Kind) -> bool {
	for p in q.pipes {
		if p.kind == kind {
			return true
		}
	}
	return false
}

format_query_debug :: proc(q: ^Query) -> string {
	b := strings.builder_make()
	fmt.sbprintf(&b, "range=[%d,%d] pipes=%d", q.start, q.end, len(q.pipes))
	return strings.to_string(b)
}
