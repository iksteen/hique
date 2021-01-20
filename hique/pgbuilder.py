from __future__ import annotations

from functools import partial
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from hique.base import FieldAttrDescriptor
from hique.expr import Expr
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
    precedence: List[Set[Optional[str]]] = [
        {"field", "literal", "arg", "call"},
        {"pos", "neg"},
        {"mul", "div", "mod"},
        {"add", "sub"},
        {"is_null"},
        {"is_not_null"},
        {None},
        {"lt", "gt", "eq", "le", "ge", "ne"},
        {"invert"},
        {"and"},
        {"or"},
    ]
    precedence_map: Dict[Optional[str], int]

    def __init__(self) -> None:
        self.precedence_map = {}
        for i, op_names in enumerate(self.precedence):
            for op_name in op_names:
                self.precedence_map[op_name] = i

    def __call__(self, query: Query) -> Tuple[str, List[Any]]:
        args = Args()
        if isinstance(query, SelectQuery):
            return self.select(query, args), args.args
        raise NotImplementedError

    def select(self, query: SelectQuery, args: Args) -> str:
        values = []
        for value in query._values:
            alias: Optional[str]
            if isinstance(value, FieldAttrDescriptor):
                alias = f"{value.table.__alias__}.{value.__alias__ or value.field.name}"
            else:
                alias = value.__alias__ or None
            if alias is None:
                values.append(f"{self.emit(value, args)}")
            else:
                values.append(f"{self.emit(value, args)} AS {self.dot_quote(alias)}")

        if query._from:
            from_set = set()
            froms = []
            for from_entry in query._from:
                from_table = from_entry.__table_name__
                from_alias = from_entry.__alias__
                if (from_table, from_alias) not in from_set:
                    from_set.add((from_table, from_alias))
                    if from_table != from_alias:
                        froms.append(f"{from_table} AS {from_alias}")
                    else:
                        froms.append(from_table)
            from_ = f" FROM {', '.join(froms)}"
        else:
            from_ = ""

        if query._filter:
            where = (
                f" WHERE {' AND '.join([self.emit(f, args) for f in query._filter])}"
            )
        else:
            where = ""

        return f"SELECT {', '.join(values)}{from_}{where}"

    def get_precedence(self, expr: Any) -> int:
        if not isinstance(expr, Expr):
            return self.precedence_map["arg"]

        precedence = self.precedence_map.get(expr.op)
        if precedence is None:
            return self.precedence_map[None]
        return precedence

    def dot_quote(self, *parts: Optional[str]) -> str:
        return ".".join(f'"{part}"' for part in parts)

    def emit(self, expr: Any, args: Args) -> str:
        if isinstance(expr, Expr):
            return self.emit_expr(expr, args)
        else:
            return self.emit_arg(expr, args)

    def emit_arg(self, value: object, args: Args) -> str:
        return f"${args(value)}"

    def emit_expr(self, expr: Expr, args: Args) -> str:
        f = self.expr_emitter_map.get(expr.op)
        if f is None:
            raise NotImplementedError(expr.op)
        return f(self, expr, args)

    def emit_field(self, expr: Expr, args: Args) -> str:
        assert isinstance(expr, FieldAttrDescriptor)
        return self.dot_quote(expr.table.__alias__, expr.field.name)

    def emit_call(
        self,
        name: str,
        call_args: Sequence[Any],
        args: Args,
        *,
        schema: Optional[str] = None,
    ) -> str:
        if schema is not None:
            f = f"{schema}.{name}"
        else:
            f = name
        return f"{f}({', '.join([self.emit(e, args) for e in call_args])})"

    def emit_call_expr(self, expr: Expr, args: Args) -> str:
        schema, name, call_args = expr.args
        return self.emit_call(name, call_args, args, schema=schema)

    def emit_op(
        self,
        expr: Expr,
        args: Args,
        *,
        prefix: str = "",
        infix: str = "",
        suffix: str = "",
    ) -> str:
        precedence = self.get_precedence(expr)

        args_sql = []
        for arg in expr.args:
            arg_sql = self.emit(arg, args)
            arg_precedence = self.get_precedence(arg)
            if precedence < arg_precedence:
                arg_sql = f"({arg_sql})"
            args_sql.append(arg_sql)

        return f"{prefix}{infix.join(args_sql)}{suffix}"

    def not_implemented(self, expr: Expr, args: Args) -> str:
        raise NotImplementedError(expr)

    expr_emitter_map: Dict[str, Callable[[PostgresqlQueryBuilder, Expr, Args], str]] = {
        "literal": lambda s, e, a: str(e.args[0]),
        "field": emit_field,
        "lt": partial(emit_op, infix=" < "),
        "le": partial(emit_op, infix=" <= "),
        "eq": partial(emit_op, infix=" = "),
        "ne": partial(emit_op, infix=" <> "),
        "gt": partial(emit_op, infix=" > "),
        "ge": partial(emit_op, infix=" >= "),
        "neg": partial(emit_op, prefix="-"),
        "pos": partial(emit_op, prefix="+"),
        "abs": lambda self, e, a: self.emit_call("abs", e.args, a),
        "invert": partial(emit_op, prefix="NOT "),
        "add": partial(emit_op, infix=" + "),
        "sub": partial(emit_op, infix=" - "),
        "mul": partial(emit_op, infix=" * "),
        "matmul": not_implemented,
        "div": partial(emit_op, infix=" / "),
        "floordiv": lambda self, e, a: self.emit_call("div", e.args, a),
        "mod": partial(emit_op, infix=" % "),
        "divmod": not_implemented,
        "pow": lambda self, e, a: self.emit_call("power", e.args, a),
        "lshift": partial(emit_op, infix=" << "),
        "rshift": partial(emit_op, infix=" >> "),
        "and": partial(emit_op, infix=" AND "),
        "xor": partial(emit_op, infix=" # "),
        "or": partial(emit_op, infix=" OR "),
        "round": lambda self, e, a: self.emit_call("round", e.args, a),
        "floor": lambda self, e, a: self.emit_call("floor", e.args, a),
        "ceil": lambda self, e, a: self.emit_call("ceil", e.args, a),
        "is_null": partial(emit_op, suffix=" IS NULL"),
        "is_not_null": partial(emit_op, suffix=" IS NOT NULL"),
        "call": emit_call_expr,
    }
