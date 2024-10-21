package main

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
)

// evaluating expressions
type QLEvalContext struct {
	env Record
	out Value
	err error
}

func qlErr(ctx *QLEvalContext, format string, args ...interface{}) {
	if ctx.err == nil {
		ctx.out.Type = QL_ERR
		ctx.err = fmt.Errorf(format, args...)
	}
}

func b2i(b bool) int64 {
	if b {
		return 1
	} else {
		return 0
	}
}

func qlEval(ctx *QLEvalContext, node QLNODE) {
	switch node.Type {
	// refer to col.
	case QL_SYM:
		if v := ctx.env.Get(string(node.Str)); v != nil {
			ctx.out = *v
		} else {
			qlErr(ctx, "unknown col.: %s", node.Str)
		}
	//literla value
	case QL_I64, QL_STR:
		ctx.out = node.Value
	case QL_TUP:
		qlErr(ctx, "unexpected tuple")

		// operators
	case QL_NEG:
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			ctx.out.I64 = -ctx.out.I64
		} else {
			qlErr(ctx, "QL_NEG type error")
		}
	case QL_NOT:
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			ctx.out.I64 = b2i(ctx.out.I64 == 0)
		} else {
			qlErr(ctx, "QL_NOT type error")
		}

		// binary ops.
	case QL_CMP_GE, QL_CMP_GT, QL_CMP_LT, QL_CMP_LE, QL_CMP_EQ, QL_CMP_NE:
		fallthrough
	case QL_ADD, QL_SUB, QL_MUL, QL_DIV, QL_MOD, QL_AND, QL_OR:
		qlBinop(ctx, node)

	default:
		panic("unreachable")

	}
}

func qlBinopI64(ctx *QLEvalContext, op uint32, a1 int64, a2 int64) int64 {
	switch op {
	case QL_ADD:
		return a1 + a2
	case QL_SUB:
		return a1 - a2
	case QL_MUL:
		return a1 * a2
	case QL_DIV:
		if a2 == 0 {
			qlErr(ctx, "div. by zero")
			return 0
		}
		return a1 / a2

	case QL_MOD:
		if a2 == 0 {
			qlErr(ctx, "div. by zero")
			return 0
		}

		return a1 % a2

	case QL_AND:
		return b2i(a1&a2 != 0)
	case QL_OR:
		return b2i(a1|a2 != 0)

	default:
		qlErr(ctx, "bad i64 binop")
		return 0
	}
}

func qlBinopStr(ctx *QLEvalContext, op uint32, a1 []byte, a2 []byte) {
	switch op {
	case QL_ADD:
		ctx.out.Type =TYPE_BYTES
		ctx.out.Str = slices.Concat(a1, a2)
	default:
		qlErr(ctx, "bad str binop")
	}
}

// binary operators
func qlBinop(ctx *QLEvalContext, node QLNODE) {
	isCmp := false
	switch node.Type {
	case QL_CMP_GE, QL_CMP_GT, QL_CMP_LT, QL_CMP_LE, QL_CMP_EQ, QL_CMP_NE:
		isCmp = true
	}

	// tuple comparision
	if isCmp && node.Kids[0].Type == QL_TUP && node.Kids[1].Type == QL_TUP {
		r := qlTupleCmp(ctx, node.Kids[0], node.Kids[1])
		ctx.out.Type = QL_I64
		ctx.out.I64 = b2i(cmp2bool(r, node.Type))
		return
	}

	// subexpressions
	qlEval(ctx, node.Kids[0])
	a1 := ctx.out
	qlEval(ctx, node.Kids[1])
	a2 := ctx.out

	// scalar comparision
	if isCmp {
		r := qlValueCmp(ctx, a1, a2)
		ctx.out.Type = QL_I64
		ctx.out.I64 = b2i(cmp2bool(r, node.Type))
		return
	}

	switch {
	case ctx.err != nil:
		return
	case a1.Type == TYPE_INT64:
		ctx.out.Type = QL_I64
		ctx.out.I64 = qlBinopI64(ctx, node.Type, a1.I64, a2.I64)
	
	case a1.Type != a2.Type:
		qlErr(ctx, "binop type mismatch")

	case a1.Type == TYPE_BYTES:
		ctx.out.Type = QL_STR
		qlBinopStr(ctx, node.Type, a1.Str, a2.Str)

	default:
		panic("unreachable")
	}

}

func cmp2bool(res int, cmd uint32) bool {
	switch cmd {
	case QL_CMP_GE:
		return res >= 0
	case QL_CMP_GT:
		return res > 0
	case QL_CMP_LT:
		return res < 0
	case QL_CMP_LE:
		return res <= 0
	case QL_CMP_EQ:
		return res == 0
	case QL_CMP_NE:
		return res != 0

	default:
		panic("unreachable")
	}
}

func qlValueCmp(ctx *QLEvalContext, a1 Value, a2 Value) int {
	switch {
	case ctx.err != nil:
		return 0

	case a1.Type != a2.Type:
		qlErr(ctx, "comparison of different types")
		return 0

	case a1.Type == TYPE_INT64:
		return cmp.Compare(a1.I64, a2.I64)

	case a1.Type == TYPE_BYTES:
		return bytes.Compare(a1.Str, a2.Str)

	default:
		panic("unreachable")
	}
}

// comparin 2 tuples of equal length
func qlTupleCmp(ctx *QLEvalContext, n1 QLNODE, n2 QLNODE) int {
	if len(n1.Kids) != len(n2.Kids) {
		qlErr(ctx, "tuple comp. of different lengths")
	}

	for i := 0; i < len(n1.Kids) && ctx.err == nil; i++ {
		qlEval(ctx, n1.Kids[i])
		a1 := ctx.out
		qlEval(ctx, n2.Kids[i])
		a2 := ctx.out
		if cmp := qlValueCmp(ctx, a1, a2); cmp != 0 {
			return cmp
		}
	}

	return 0
}
