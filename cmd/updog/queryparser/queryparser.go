package queryparser

import (
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"strings"
	"unicode/utf8"

	"github.com/akrennmair/updog/proto"
)

// query syntax:
// query ::= expr [ ';' field-list ]
// expr ::= simple-expr | and-expr | or-expr
// simple-expr ::= grouped-expr | not-expr | comparison.
// grouped-expr ::= '(' expr ')'.
// and-expr ::= simple-expr { '&' simple-expr }.
// or-expr ::= simple-expr { '|' simple-expr }.
// not-expr ::= '^' simple-expr.
// comparison ::= field '=' value.
// field-list ::= field { ',' field } .

func ParseQuery(q string) (pq *proto.Query, err error) {
	p := newParser(q)

	defer p.recover(&err)

	pq, err = p.parse()
	return pq, err
}

type parser struct {
	lexer     *lexer
	logger    *log.Logger
	token     [3]item
	peekCount int
}

func newParser(text string) *parser {
	return &parser{
		lexer:  lex(text),
		logger: log.New(io.Discard, "parser", log.LstdFlags|log.Lshortfile),
	}
}

func (p *parser) recover(errp *error) {
	e := recover()
	if e != nil {
		// rethrow runtime errors
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		*errp = e.(error)
	}
}

func (p *parser) errorf(fmtstr string, args ...interface{}) {
	err := errors.New(fmt.Sprintf("%d:%d: ", p.lexer.lineNumber(), p.lexer.columnInLine()) + fmt.Sprintf(fmtstr, args...))
	panic(err)
}

func (p *parser) parse() (pq *proto.Query, err error) {
	defer p.recover(&err)

	// query ::= expr [ ';' field-list ]

	expr := p.parseExpr()

	var groupBy []string

	if p.peek().typ == itemSemicolon {
		p.next()
		groupBy = p.parseFieldList()

	}

	pq = &proto.Query{
		Expr:    expr,
		GroupBy: groupBy,
	}

	return pq, nil
}

func (p *parser) peek() item {
	if p.peekCount > 0 {
		return p.token[p.peekCount-1]
	}
	p.peekCount = 1
	p.token[0] = p.lexer.nextItem()
	return p.token[0]
}

func (p *parser) next() item {
	if p.peekCount > 0 {
		p.peekCount--
	} else {
		p.token[0] = p.lexer.nextItem()
	}
	i := p.token[p.peekCount]
	return i
}

func (p *parser) parseExpr() *proto.Query_Expression {
	// expr ::= simple-expr | and-expr | or-expr

	expr := p.parseSimpleExpr()

	switch p.peek().typ {
	case itemAnd:
		return p.parseAndExpr(expr)
	case itemOr:
		return p.parseOrExpr(expr)
	}

	return expr
}

func (p *parser) parseAndExpr(firstExpr *proto.Query_Expression) *proto.Query_Expression {
	// and-expr ::= simple-expr { '&' simple-expr }.

	exprs := []*proto.Query_Expression{firstExpr}

	for p.peek().typ == itemAnd {
		p.next()

		expr := p.parseSimpleExpr()

		exprs = append(exprs, expr)
	}

	return &proto.Query_Expression{
		Value: &proto.Query_Expression_And_{
			And: &proto.Query_Expression_And{
				Exprs: exprs,
			},
		},
	}
}

func (p *parser) parseOrExpr(firstExpr *proto.Query_Expression) *proto.Query_Expression {
	// or-expr ::= simple-expr { '|' simple-expr }.

	exprs := []*proto.Query_Expression{firstExpr}

	for p.peek().typ == itemOr {
		p.next()

		expr := p.parseSimpleExpr()

		exprs = append(exprs, expr)
	}

	return &proto.Query_Expression{
		Value: &proto.Query_Expression_Or_{
			Or: &proto.Query_Expression_Or{
				Exprs: exprs,
			},
		},
	}
}

func (p *parser) parseSimpleExpr() *proto.Query_Expression {
	switch p.peek().typ {
	case itemOpenParen:
		return p.parseGroupedExpr()
	case itemNot:
		// not-expr ::= '^' simple-expr.
		p.next()
		return &proto.Query_Expression{
			Value: &proto.Query_Expression_Not_{
				Not: &proto.Query_Expression_Not{
					Expr: p.parseSimpleExpr(),
				},
			},
		}
	case itemField:
		return p.parseComparison()
	default:
		p.errorf("unexpected token %q", p.peek())
		return nil
	}
}

func (p *parser) parseGroupedExpr() *proto.Query_Expression {
	// grouped-expr ::= '(' expr ')'.

	p.next() // skip open parenthesis; this has already been checked when the method was called.

	expr := p.parseExpr()

	if p.peek().typ != itemCloseParen {
		p.errorf("expected ), got %s instead", p.next())
	}
	p.next()

	return expr
}

func (p *parser) parseComparison() *proto.Query_Expression {
	column := p.next()

	if p.peek().typ != itemEqual {
		p.errorf("expected =, got %s instead", p.next())
	}
	p.next()

	if p.peek().typ != itemValue {
		p.errorf("expected value, got %s instead", p.next())
	}

	value := p.next()

	return &proto.Query_Expression{
		Value: &proto.Query_Expression_Eq{
			Eq: &proto.Query_Expression_Equal{
				Column: column.val,
				Value:  decodeString(value.val),
			},
		},
	}

}

func decodeString(s string) string {
	if len(s) < 2 {
		return s
	}

	if s[0] == '"' {
		s = s[1:]
	}

	if s[len(s)-1] == '"' {
		s = s[:len(s)-1]
	}

	return strings.ReplaceAll(s, `""`, `"`)
}

func (p *parser) parseFieldList() []string {
	if p.peek().typ != itemField {
		p.errorf("expected field, got %s instead", p.next())
	}

	var fields []string

	fields = append(fields, p.next().val)

	for p.peek().typ == itemComma {
		p.next()

		if p.peek().typ != itemField {
			p.errorf("expected field, got %s instead", p.next())
		}

		fields = append(fields, p.next().val)
	}

	return fields
}

type lexer struct {
	input   string
	state   stateFn
	pos     pos
	start   pos
	width   pos
	lastPos pos
	items   chan item
}

const eof = -1

type stateFn func(*lexer) stateFn

type item struct {
	typ itemType
	pos pos
	val string
}

type itemType int

func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	}
	return fmt.Sprintf("%q", i.val)
}

type pos int

const (
	itemError itemType = iota
	itemEOF
	itemOpenParen
	itemCloseParen
	itemAnd
	itemOr
	itemNot
	itemEqual
	itemComma
	itemSemicolon
	itemField
	itemValue
)

func lex(input string) *lexer {
	l := &lexer{
		input: input,
		items: make(chan item),
	}
	go l.run()
	return l
}

func (l *lexer) run() {
	for l.state = lexText; l.state != nil; {
		l.state = l.state(l)
	}
}

func lexText(l *lexer) stateFn {
	r := l.peek()
	switch {
	case r == ' ' || r == '\n' || r == '\r' || r == '\t':
		l.acceptRun("\r\n\t ")
		l.ignore()
		return lexText
	case r == '(':
		l.next()
		l.emit(itemOpenParen)
		return lexText
	case r == ')':
		l.next()
		l.emit(itemCloseParen)
		return lexText
	case r == '&':
		l.next()
		l.emit(itemAnd)
		return lexText
	case r == '|':
		l.next()
		l.emit(itemOr)
		return lexText
	case r == '^':
		l.next()
		l.emit(itemNot)
		return lexText
	case r == ',':
		l.next()
		l.emit(itemComma)
		return lexText
	case r == ';':
		l.next()
		l.emit(itemSemicolon)
		return lexText
	case r == '=':
		l.next()
		l.emit(itemEqual)
		return lexText
	case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		return lexField
	case r == '"':
		return lexValue
	case r == eof:
		l.emit(itemEOF)
		return nil
	}
	return l.errorf("unknown token: %s", l.input[l.pos:])
}

func lexField(l *lexer) stateFn {
	l.acceptRun("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_")
	l.emit(itemField)
	return lexText
}

func lexValue(l *lexer) stateFn {
	seenFinalQuote := false // this is only there in case the closing " is the final character in the text to parse; mostly necessary for expression parsing testing.
	r := l.next()
	if r != '"' {
		return l.errorf("expected \", got %c instead", r)
	}
	for r = l.next(); r != eof; r = l.next() {
		if r == '"' { // if the current character is ", then we peek to the next one.
			r = l.peek()
			if r != '"' { // if it also a ", then we just go to next one, otherwise we've hit the final " of a string.
				seenFinalQuote = true
				break
			}
			l.next()
		}
	}

	if seenFinalQuote || r != eof {
		l.emit(itemValue)
	}
	return lexText
}

func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

func (l *lexer) next() rune {
	if int(l.pos) >= len(l.input) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = pos(w)
	l.pos += l.width
	return r
}

func (l *lexer) nextItem() item {
	item := <-l.items
	l.lastPos = item.pos
	return item
}

func (l *lexer) backup() {
	l.pos -= l.width
}

func (l *lexer) emit(t itemType) {
	l.items <- item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
}

func (l *lexer) acceptRun(valid string) {
	for strings.ContainsRune(valid, l.next()) {
	}
	l.backup()
}

func (l *lexer) ignore() {
	l.start = l.pos
}

func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{itemError, l.start, fmt.Sprintf(format, args...)}
	return nil
}

func (l *lexer) lineNumber() int {
	return 1 + strings.Count(l.input[:l.lastPos], "\n")
}

func (l *lexer) columnInLine() int {
	bolPos := strings.LastIndex(l.input[:l.lastPos], "\n")
	return int(l.lastPos) - bolPos + 1
}
