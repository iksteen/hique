from __future__ import annotations

from functools import partial
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Type

from hique.base import FieldExpr, Model
from hique.builder import QueryBuilder
from hique.expr import CallExpr, Expr
from hique.query import BaseSelectQuery, DeleteQuery, InsertQuery, Join, JoinType, Query
from hique.util import assert_never


class Args:
    def __init__(self) -> None:
        self.args: List[Any] = []

    def __call__(self, value: Any) -> int:
        self.args.append(value)
        return len(self.args)

    def __repr__(self) -> str:
        return repr(self.args)


class PostgresqlQueryBuilder(QueryBuilder):
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

    def __call__(self, query: Query) -> Tuple[str, Tuple[Any, ...]]:
        args = Args()
        if isinstance(query, BaseSelectQuery):
            return self.select(query, args), tuple(args.args)
        elif isinstance(query, InsertQuery):
            return self.insert(query, args), tuple(args.args)
        elif isinstance(query, DeleteQuery):
            return self.delete(query, args), tuple(args.args)
        raise NotImplementedError

    def select(self, query: BaseSelectQuery, args: Args) -> str:
        def add_joins(
            join_map: Dict[Type[Model], List[Join]],
            model: Type[Model],
            result: List[str],
        ) -> None:
            for join in join_map.get(model, []):
                join_type = join.join_type
                if join_type is JoinType.INNER:
                    result.append("JOIN")
                elif join_type is JoinType.LEFT:
                    result.append("LEFT JOIN")
                elif join_type is JoinType.RIGHT:
                    result.append("RIGHT JOIN")
                elif join_type is JoinType.FULL:
                    result.append("FULL JOIN")
                elif join_type is JoinType.CROSS:
                    result.append("CROSS JOIN")
                else:
                    assert_never(join_type)

                result.append(self.quote(join.dest.__table_name__))

                if join.dest.__alias__ != join.dest.__table_name__:
                    result.append(f"AS {self.quote(join.dest.__alias__)}")

                if join_type is not JoinType.CROSS:
                    result.extend(("ON", self.emit(join.condition, args)))

                add_joins(join_map, join.dest, result)

        values = []
        for value in query._values:
            alias: Optional[str]
            if isinstance(value, FieldExpr):
                alias = f"{value.table.__alias__}.{value.__alias__ or value.descriptor.name}"
            else:
                alias = value.__alias__ or None
            if alias is None:
                values.append(f"{self.emit(value, args)}")
            else:
                values.append(f"{self.emit(value, args)} AS {self.quote(alias)}")

        if query._from:
            from_set = set()
            froms = []
            for from_entry in query._from:
                if from_entry not in from_set:
                    from_set.add(from_entry)

                    from_table = [self.quote(from_entry.__table_name__)]
                    from_alias = self.quote(from_entry.__alias__)

                    if from_table[0] != from_alias:
                        from_table.append(f"AS {from_alias}")

                    add_joins(query._join, from_entry, from_table)
                    froms.append(" ".join(from_table))
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

    def insert(self, query: InsertQuery[Any], args: Args) -> str:
        table_name = self.quote(query._model.__table_name__)
        if query._model.__alias__ != query._model.__table_name__:
            table_name = f"{table_name} AS {self.quote(query._model.__alias__)}"

        columns, values = zip(*query._values.items())
        columns_str = ", ".join(map(self.quote, columns))
        values_str = ", ".join(map(partial(self.emit, args=args), values))

        if query._returning:
            returning = []
            for value in query._returning:
                alias: Optional[str]
                if isinstance(value, FieldExpr):
                    alias = f"{value.table.__alias__}.{value.__alias__ or value.descriptor.name}"
                else:
                    alias = value.__alias__ or None
                if alias is None:
                    returning.append(f"{self.emit(value, args)}")
                else:
                    returning.append(f"{self.emit(value, args)} AS {self.quote(alias)}")
            returning_str = f" RETURNING {', '.join(returning)}"
        else:
            returning_str = ""
        return f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str}){returning_str}"

    def delete(self, query: DeleteQuery, args: Args) -> str:
        table_name = self.quote(query._model.__table_name__)
        if query._model.__alias__ != query._model.__table_name__:
            table_name = f"{table_name} AS {self.quote(query._model.__alias__)}"

        if query._filter:
            where = (
                f" WHERE {' AND '.join([self.emit(f, args) for f in query._filter])}"
            )
        else:
            where = ""

        return f"DELETE FROM {table_name}{where}"

    def get_precedence(self, expr: Any) -> int:
        if not isinstance(expr, Expr):
            return self.precedence_map["arg"]

        precedence = self.precedence_map.get(expr.op)
        if precedence is None:
            return self.precedence_map[None]
        return precedence

    def quote(
        self, *parts: Optional[str], delim: str = "", skip_none: bool = False
    ) -> str:
        return delim.join(
            f'"{part}"' for part in parts if part is not None or not skip_none
        )

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
        assert isinstance(expr, FieldExpr)
        if expr.descriptor.expr is not None:
            return self.emit(expr.descriptor.expr(), args)
        else:
            return self.quote(expr.table.__alias__, expr.descriptor.name, delim=".")

    def emit_call(
        self,
        schema: Optional[str],
        name: str,
        call_args: Sequence[Any],
        args: Args,
    ) -> str:
        if schema is not None:
            f = f"{schema}.{name}"
        else:
            f = name
        return f"{f}({', '.join([self.emit(e, args) for e in call_args])})"

    def emit_call_expr(self, expr: Expr, args: Args) -> str:
        assert isinstance(expr, CallExpr)
        return self.emit_call(expr.schema, expr.name, expr.args, args)

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
        "abs": lambda self, e, a: self.emit_call(None, "abs", e.args, a),
        "invert": partial(emit_op, prefix="NOT "),
        "add": partial(emit_op, infix=" + "),
        "sub": partial(emit_op, infix=" - "),
        "mul": partial(emit_op, infix=" * "),
        "matmul": not_implemented,
        "div": partial(emit_op, infix=" / "),
        "floordiv": lambda self, e, a: self.emit_call(None, "div", e.args, a),
        "mod": partial(emit_op, infix=" % "),
        "divmod": not_implemented,
        "pow": lambda self, e, a: self.emit_call(None, "power", e.args, a),
        "lshift": partial(emit_op, infix=" << "),
        "rshift": partial(emit_op, infix=" >> "),
        "and": partial(emit_op, infix=" AND "),
        "xor": partial(emit_op, infix=" # "),
        "or": partial(emit_op, infix=" OR "),
        "round": lambda self, e, a: self.emit_call(None, "round", e.args, a),
        "floor": lambda self, e, a: self.emit_call(None, "floor", e.args, a),
        "ceil": lambda self, e, a: self.emit_call(None, "ceil", e.args, a),
        "is_null": partial(emit_op, suffix=" IS NULL"),
        "is_not_null": partial(emit_op, suffix=" IS NOT NULL"),
        "call": emit_call_expr,
    }
