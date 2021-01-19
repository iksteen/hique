from __future__ import annotations

from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple, cast

from hique.base import FieldAttrDescriptor
from hique.expr import BinOpExpr, CallExpr, Expr, Literal, UnOpExpr
from hique.query import Query, SelectQuery


class Args:
    def __init__(self) -> None:
        self.args: List[Any] = []

    def __call__(self, value: Any) -> int:
        self.args.append(value)
        return len(self.args)

    def __repr__(self) -> str:
        return repr(self.args)


class PostgresqlQueryBuilder:
    def __call__(self, query: Query) -> Tuple[str, Args]:
        args = Args()
        if isinstance(query, SelectQuery):
            return self.select(query, args), args
        raise NotImplementedError

    def select(self, query: SelectQuery, args: Args) -> str:
        where = [self.expr(f, args) for f in query._filter]
        return f"SELECT * FROM ... WHERE {' AND '.join(where)}"

    def expr(self, expr: Any, args: Args) -> str:
        for t in type(expr).mro():
            t_f = self.expr_type_map.get(t)
            if t_f is not None:
                return t_f(self, expr, args)
        else:
            raise NotImplementedError(type(expr))

    def quote(self, *parts: Optional[str]) -> str:
        return ".".join(f'"{part}"' for part in parts)

    def emit_arg(self, value: object, args: Args) -> str:
        return f"${args(value)}"

    def emit_field(self, field: FieldAttrDescriptor[Any], args: Args) -> str:
        return self.quote(
            field.table.__alias__ or field.table.__table_name__, field.field.name
        )

    def emit_un_op(self, expr: UnOpExpr, args: Args) -> str:
        f = self.un_op_expr_map.get(expr.op)
        if f is not None:
            return f(self, expr, args)
        raise NotImplementedError(expr.op)

    def emit_bin_op(self, expr: BinOpExpr, args: Args) -> str:
        f = self.bin_op_expr_map.get(expr.op)
        if f is not None:
            return f(self, expr, args)
        raise NotImplementedError(expr.op)

    def emit_call(self, expr: CallExpr, args: Args) -> str:
        if expr.schema is not None:
            f = f"{expr.schema}.{expr.name}"
        else:
            f = expr.name
        return f"{f}({', '.join([self.expr(e, args) for e in expr.args])})"

    def emit_infix_op(self, expr: BinOpExpr, args: Args, *, infix: str) -> str:
        return f"({self.expr(expr.left, args)}{infix}{self.expr(expr.right, args)})"

    def emit_prefix_op(self, expr: UnOpExpr, args: Args, *, prefix: str) -> str:
        return f"{prefix}{expr.value}"

    def emit_postfix_op(self, expr: UnOpExpr, args: Args, *, postfix: str) -> str:
        return f"{self.expr(expr.value, args)}{postfix}"

    def not_implemented(self, expr: Expr, args: Args) -> str:
        raise NotImplementedError(expr.op)

    expr_type_map: Dict[type, Callable[[PostgresqlQueryBuilder, Any, Args], str]] = {
        object: emit_arg,
        Expr: not_implemented,
        Literal: lambda s, e, a: cast(str, e.value),
        FieldAttrDescriptor: emit_field,
        UnOpExpr: emit_un_op,
        BinOpExpr: emit_bin_op,
        CallExpr: emit_call,
    }
    un_op_expr_map: Dict[
        str, Callable[[PostgresqlQueryBuilder, UnOpExpr, Args], str]
    ] = {
        "neg": partial(emit_prefix_op, prefix="-"),
        "pos": partial(emit_prefix_op, prefix="+"),
        "abs": lambda self, e, a: self.emit_call(CallExpr("abs", e.value), a),
        "is_null": partial(emit_postfix_op, postfix=" IS NULL"),
        "is_not_null": partial(emit_postfix_op, postfix=" IS NOT NULL"),
    }
    bin_op_expr_map: Dict[
        str, Callable[[PostgresqlQueryBuilder, BinOpExpr, Args], str]
    ] = {
        "lt": partial(emit_infix_op, infix=" < "),
        "le": partial(emit_infix_op, infix=" <= "),
        "eq": partial(emit_infix_op, infix=" == "),
        "ne": partial(emit_infix_op, infix=" != "),
        "gt": partial(emit_infix_op, infix=" > "),
        "ge": partial(emit_infix_op, infix=" >= "),
        "or": partial(emit_infix_op, infix=" OR "),
        "and": partial(emit_infix_op, infix=" AND "),
        "add": partial(emit_infix_op, infix=" + "),
        "sub": partial(emit_infix_op, infix=" - "),
    }
