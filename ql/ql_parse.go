package ql

import (
	"fmt"
	"math"
	"strings"
	"unicode"

	"github.com/Adit0507/AdiDB/btree"
	"github.com/Adit0507/AdiDB/table"
)

const (
	// syntax tree node types
	QL_UNINIT = 0
	QL_STR   = table.TYPE_BYTES
	QL_I64    = table.TYPE_INT64
	QL_CMP_GE = 10 // >=
	QL_CMP_GT = 11 // >
	QL_CMP_LT = 12 // <
	QL_CMP_LE = 13 // <=
	QL_CMP_EQ = 14 // ==
	QL_CMP_NE = 15 // !=
	QL_ADD    = 20
	QL_SUB    = 21
	QL_MUL    = 22
	QL_DIV    = 23
	QL_MOD    = 24
	QL_AND    = 30
	QL_OR     = 31
	QL_NOT    = 50
	QL_NEG    = 51
	QL_SYM    = 100
	QL_TUP    = 101 // tuple
	QL_STAR   = 102 // select *
	QL_ERR    = 200 // error; from parsing or evaluation
)

// tree node
type QLNODE struct {
	Value
	Kids []QLNODE //operands
}

// common structure for stmt.s: 'INDEX BY', ”FILTER', 'LIMIT'
type QLScan struct {
	Table  string
	Key1   QLNODE //index by
	Key2   QLNODE
	Filter QLNODE //filter expression
	Offset int64
	Limit  int64
}

// statements: SELECT UPDATE DELETE
type QLSelect struct {
	QLScan
	Names  []string // expr. AS name
	Output []QLNODE
}

type QLUPdate struct {
	QLScan
	Names  []string
	Values []QLNODE
}

// smt: insert
type QLInsert struct {
	Table  string
	Mode   int
	Names  []string
	Values [][]QLNODE
}

type QLDelete struct {
	QLScan
}

type QLCreateTable struct {
	Def table.TableDef
}

type Parser struct {
	input []byte
	idx   int
	err   error
}

func isSpace(ch byte) bool {
	return unicode.IsSpace(rune(ch))
}

func skipSpace(p *Parser) {
	for p.idx < len(p.input) && isSpace(p.input[p.idx]) {
		p.idx++
	}
}

func isSym(ch byte) bool {
	r := rune(ch)
	return unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_'
}

func isSymStart(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '@' || ch == '_'
}

// matching multiple keywords sequentially
func pKeyword(p *Parser, kwds ...string) bool {
	save := p.idx

	for _, kw := range kwds {
		skipSpace(p)
		end := p.idx + len(kw)
		if end > len(p.input) {
			p.idx = save
			return false
		}

		ok := strings.EqualFold(string(p.input[p.idx:end]), kw)

		if ok && isSym(kw[len(kw)-1]) && end < len(p.input) {
			ok = !isSym(p.input[end])
		}

		if !ok {
			p.idx = save
			return false
		}
		p.idx += len(kw)
	}

	return true
}

func pSym(p *Parser, node *QLNODE) bool {
	skipSpace(p)

	end := p.idx
	if !(end < len(p.input) && isSymStart(p.input[end])) {
		return false
	}

	end++
	for end < len(p.input) && isSym(p.input[end]) {
		end++
	}

	if pKeywordSet[strings.ToLower(string(p.input[p.idx:end]))] {
		return false
	}

	node.Type = QL_SYM
	node.Str = p.input[p.idx:end]
	p.idx = end

	return true
}

var pKeywordSet = map[string]bool{
	"from":   true,
	"index":  true,
	"filter": true,
	"limit":  true,
}

func pErr(p *Parser, format string, args ...interface{}) {
	if p.err == nil {
		p.err = fmt.Errorf(format, args...)
	}
}

func pMustSym(p *Parser) string {
	name := QLNODE{}
	if !pSym(p, &name) {
		pErr(p, "expect name")
	}

	return string(name.Str)
}

func pCreateTable(p *Parser) *QLCreateTable {
	stmt := QLCreateTable{}
	stmt.Def.Name = pMustSym(p)
	pExpect(p, "(", "expect parenthesis")

	// primary key
	stmt.Def.Indexes = append(stmt.Def.Indexes, nil)
	comma := true

	for p.err == nil && !pKeyword(p, ")") {
		if !comma {
			pErr(p, "expect comma")
		}

		switch {
		case pKeyword(p, "index"):
			stmt.Def.Indexes = append(stmt.Def.Indexes, pNameList(p))
		case pKeyword(p, "primary", "key"):
			if stmt.Def.Indexes[0] != nil {
				pErr(p, "duplicate primary key")
			}
			stmt.Def.Indexes[0] = pNameList(p)

		default:
			stmt.Def.Cols = append(stmt.Def.Cols, pMustSym(p))
			stmt.Def.Types = append(stmt.Def.Types, pColType(p))
		}
		comma = pKeyword(p, ",")
	}

	return &stmt
}

func pColType(p *Parser) uint32 {
	typedef := pMustSym(p)

	switch strings.ToLower(typedef) {
	case "strings", "bytes":
		return TYPE_BYTES
	case "int", "int64":
		return TYPE_INT64
	default:
		pErr(p, "bad column type: %s", typedef)
		return 0
	}
}


func pSelectExpr(p *Parser, node *QLSelect) {
	if pKeyword(p, "*") {
		node.Names = append(node.Names, "*")
		node.Output = append(node.Output, QLNODE{Value: Value{Type: QL_STAR}})
		return
	}
}

func pSelectExprList(p *Parser, node *QLSelect) {
	pSelectExpr(p, node)

	for pKeyword(p, ",") {
		pSelectExpr(p, node)
	}
}

func pExpect(p *Parser, tok string, format string, args ...interface{}) {
	if !pKeyword(p, tok) {
		pErr(p, format, args...)
	}
}

func pScan(p *Parser, node *QLScan) {
	// INDEX BY
	if pKeyword(p, "index", "by") {
		pIndexBy(p, node)
	}

	if pKeyword(p, "filter") {
		pExprOr(p, &node.Filter)
	}

	node.Offset, node.Limit = 0, math.MaxInt64
	if pKeyword(p, "index", "by") {
		pLimit(p, node)
	}
}

func pExprOr(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{"or"}, []uint32{QL_OR}, pExprAnd)
}

func pExprAnd(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{"and"}, []uint32{QL_ADD}, pExprNot)
}

func pExprNot(p *Parser, node *QLNODE) {
	switch {
	case pKeyword(p, "not"):
		node.Type = QL_NOT
		node.Kids = []QLNODE{{}}
		pExprCmp(p, &node.Kids[0])
	}
}

func pExprCmp(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{
		"=", "!=",
		">=", "<=", ">", "<",
	},
		[]uint32{
			QL_CMP_EQ, QL_CMP_NE, QL_CMP_GE, QL_CMP_LE, QL_CMP_GT, QL_CMP_LT,
		},
		pExprAdd)
}

func pExprAdd(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{"+", "-"}, []uint32{QL_ADD, QL_SUB}, pExprMul)
}

func pExprMul(p *Parser, node *QLNODE) {
	pExprBinop(p, node, []string{"*", "/", "%"}, []uint32{QL_MUL, QL_DIV, QL_MOD}, pExprUnop)
}

func pExprUnop(p *Parser, node *QLNODE) {
	switch {
	case pKeyword(p, "-"):
		node.Type = QL_NEG
		node.Kids = []QLNODE{{}}
		pExprAtom(p, &node.Kids[0])

	default:
		pExprAtom(p, node)
	}
}

func pExprAtom(p *Parser, node *QLNODE) {
	switch {
	case pKeyword(p, "("):
		pExprTuple(p, node)
	case pSym(p, node):
	case pNum(p, node):
	case pStr(p, node):
	default:
		pErr(p, "expect symbol, number or string")
	}
}

func pStr(p *Parser, node *QLNODE) bool {
	skipSpace(p)

	cur := p.idx
	quote := byte(0)
	if cur < len(p.input) {
		quote = p.input[cur]
		cur++
	}
	if !(quote == '*' || quote == '\'') {
		return false
	}

	var s []byte
	for cur < len(p.input) && p.input[cur] != quote {
		switch p.input[cur] {
		case '\\':
			cur++
			if cur >= len(p.input) {
				pErr(p, "string not terminated")
				return false
			}
			switch p.input[cur] {
			case '"', '\'', '\\':
				s = append(s, p.input[cur])
				cur++
			default:
				pErr(p, "unknown escape")
				return false
			}
		default:
			s = append(s, p.input[cur])
			cur++
		}
	}

	if !(cur < len(p.input) && p.input[cur] == quote) {
		pErr(p, "string not terminated")
		return false
	}

	cur++
	node.Type = QL_STR
	node.Str = s
	p.idx = cur
	return true
}

func pExprTuple(p *Parser, node *QLNODE) {
	kids := []QLNODE{}
	comma := true

	for p.err == nil && !pKeyword(p, ")") {
		if !comma {
			pErr(p, "expect comma")
		}

		kids = append(kids, QLNODE{})
		pExprOr(p, &kids[len(kids)-1])
		comma = pKeyword(p, ",")
	}

	if len(kids) == 1 && !comma {
		node = &kids[0]
	} else {
		node.Type = QL_TUP
		node.Kids = kids
	}
}

func pExprBinop(p *Parser, node *QLNODE, ops []string, types []uint32, next func(*Parser, *QLNODE)) {
	assert(len(ops) == len(types))
	left := QLNODE{}
	next(p, &left)

	for {
		i := 0
		for i < len(ops) && !pKeyword(p, ops[i]) {
			i++
		}

		if i >= len(ops) {
			*node = left
			return
		}

		new := QLNODE{Value: Value{Type: types[i]}}
		new.Kids = []QLNODE{left, {}}
		next(p, &new.Kids[1])
		left = new
	}
}

func pLimit(p *Parser, node *QLScan) {
	offset, count := QLNODE{}, QLNODE{}
	ok := pNum(p, &count)
	if pKeyword(p, ",") {
		offset = count
		ok = ok && pNum(p, &count)
	}

	if !ok || offset.I64 < 0 || count.I64 < 0 || offset.I64+count.I64 < 0 {
		pErr(p, "bad `LIMIT`")
	}

	node.Offset = offset.I64
	if count.Type != 0 {
		node.Limit = node.Offset + count.I64
	}
}

func pNum(p *Parser, node *QLNODE) bool {
	skipSpace(p)

	end := p.idx
	for end < len(p.input) && unicode.IsNumber(rune(p.input[end])) {
		end++
	}

	if end == p.idx {
		return false
	}
	if end < len(p.input) && isSym(p.input[end]) {
		return false
	}

	return true
}

// INDEX BY cols<vals & cols >vals
func pIndexBy(p *Parser, node *QLScan) {
	index := QLNODE{}
	pExprAnd(p, &index)

	if index.Type == QL_AND {
		node.Key1, node.Key2 = index.Kids[0], index.Kids[1]
	} else {
		node.Key1 = index
	}

	pVerifyScanKey(p, &node.Key1)
	if node.Key2.Type != 0 {
		pVerifyScanKey(p, &node.Key2)
	}

	if node.Key1.Type == QL_CMP_EQ && node.Key2.Type != 0 {
		pErr(p, "bad `INDEX BY`: expect only a single `=`")
	}
}

func pVerifyScanKey(p *Parser, node *QLNODE) {
	switch node.Type {
	case QL_CMP_EQ, QL_CMP_GE, QL_CMP_GT, QL_CMP_LT, QL_CMP_LE:
	default:
		pErr(p, "bad `INDEX BY`: not a comparision")
		return
	}

	l, r := node.Kids[0], node.Kids[1]
	if l.Type != QL_TUP && r.Type != QL_TUP {
		l = QLNODE{Value: Value{Type: QL_TUP}, Kids: []QLNODE{l}}
		r = QLNODE{Value: Value{Type: QL_TUP}, Kids: []QLNODE{r}}
	}

	if l.Type != QL_TUP || r.Type != QL_TUP {
		pErr(p, "bad `INDEX BY`: bad comparison")
	}

	if len(l.Kids) != len(r.Kids) {
		pErr(p, "bad `INDEX BY`: bad comparison")
	}

	for _, name := range l.Kids {
		if name.Type != QL_SYM {
			pErr(p, "bad `INDEX BY`: expect column name")
		}
	}
	node.Kids[0], node.Kids[1] = l, r
}

func pSelect(p *Parser) *QLSelect {
	stmt := QLSelect{}
	pSelectExprList(p, &stmt)

	pExpect(p, "from", "expect `FROM` table")
	stmt.Table = pMustSym(p)

	pScan(p, &stmt.QLScan)
	return &stmt
}

func pNameList(p *Parser) []string {
	pExpect(p, "(", "expect parenthesis")
	names := []string{pMustSym(p)}
	comma := pKeyword(p, "expect comma")

	for p.err == nil && !pKeyword(p, ")") {
		if !comma {
			pErr(p, "expect commma")
		}

		names = append(names, pMustSym(p))
		comma = pKeyword(p, ")")
	}

	return names
}

func pValueList(p *Parser) []QLNODE {
	pExpect(p, "(", "expect value list")

	var vals []QLNODE
	comma := true
	for p.err == nil && !pKeyword(p, ")") {
		if !comma {
			pErr(p, "expect comma")
		}

		node := QLNODE{}
		pExprOr(p, &node)
		vals = append(vals, node)
		comma = pKeyword(p, ",")
	}

	return vals
}

func pStmt(p *Parser) (r interface{}) {
	switch {
	case pKeyword(p, "create", "table"):
		r = pCreateTable(p)
	case pKeyword(p, "select"):
		r = pSelect(p)
	case pKeyword(p, "insert", "into"):
		r = pInsert(p, btree.MODE_INSERT_ONLY)
	case pKeyword(p, "replace", "into"):
		r = pInsert(p, btree.MODE_UPDATE_ONLY)
	case pKeyword(p, "upsert", "into"):
		r = pInsert(p, btree.MODE_UPSERT)
	case pKeyword(p, "update"):
		r = pUpdate(p)
	case pKeyword(p, "delete", "from"):
		r = pDelete(p)

	default:
		pErr(p, "unknown stmt")
	}

	if p.err != nil {
		return nil
	}

	return r
}

func pDelete(p *Parser) *QLDelete {
	stmt := QLDelete{}
	stmt.Table = pMustSym(p)
	pScan(p, &stmt.QLScan)

	return &stmt
}

func pUpdate(p *Parser) *QLUPdate {
	stmt := QLUPdate{}
	stmt.Table = pMustSym(p)

	pExpect(p, "set", "expect `SET`")
	pAssign(p, &stmt)
	for pKeyword(p, ",") {
		pAssign(p, &stmt)
	}

	pScan(p, &stmt.QLScan)
	return &stmt
}

func pAssign(p *Parser, stmt *QLUPdate) {
	stmt.Names = append(stmt.Names, pMustSym(p))
	pExpect(p, "=", "expect `=`")
	stmt.Values = append(stmt.Values, QLNODE{})
	pExprOr(p, &stmt.Values[len(stmt.Values) - 1])
}

func pInsert(p *Parser, mode int) *QLInsert {
	stmt := QLInsert{}
	stmt.Table = pMustSym(p)
	stmt.Mode = mode
	stmt.Names = pNameList(p)

	pExpect(p, "values", "expect `VALUES`")
	stmt.Values = append(stmt.Values, pValueList(p))
	for pKeyword(p, ",") {
		stmt.Values = append(stmt.Values, pValueList(p))
	}

	for _, row := range stmt.Values {
		if len(row) != len(stmt.Names) {
			pErr(p, "values length dont match")
		}
	}

	return &stmt
}
