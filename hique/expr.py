from __future__ import annotations

from typing import Any, Optional


class Expr:
    op: str = ""
    _alias: Optional[str] = None

    def alias(self, alias: str) -> Expr:
        self._alias = alias
        return self

    def __lt__(self, other: Any) -> BinOpExpr:
        return BinOpExpr("lt", self, other)

    def __le__(self, other: Any) -> BinOpExpr:
        return BinOpExpr("le", self, other)

    def __eq__(self, other: Any) -> BinOpExpr:  # type: ignore
        return BinOpExpr("eq", self, other)

    def __ne__(self, other: Any) -> BinOpExpr:  # type: ignore
        return BinOpExpr("ne", self, other)

    def __gt__(self, other: Any) -> BinOpExpr:
        return BinOpExpr("gt", self, other)

    def __ge__(self, other: Any) -> BinOpExpr:
        return BinOpExpr("ge", self, other)

    def __neg__(self) -> UnOpExpr:
        return UnOpExpr("neg", self)

    def __pos__(self) -> UnOpExpr:
        return UnOpExpr("pos", self)

    def __abs__(self) -> UnOpExpr:
        return UnOpExpr("abs", self)

    def __invert__(self) -> UnOpExpr:
        return UnOpExpr("invert", self)

    def __and__(self, other: Any) -> BinOpExpr:
        return BinOpExpr("and", self, other)

    def __or__(self, other: Any) -> BinOpExpr:
        return BinOpExpr("or", self, other)

    def __add__(self, other: Any) -> BinOpExpr:
        return BinOpExpr("add", self, other)

    def __sub__(self, other: Any) -> BinOpExpr:
        return BinOpExpr("sub", self, other)

    def is_null(self) -> UnOpExpr:
        return UnOpExpr("is_null", self)

    def is_not_null(self) -> UnOpExpr:
        return UnOpExpr("is_not_null", self)


class Literal(Expr):
    def __init__(self, value: str) -> None:
        self.value = value

    def __repr__(self) -> str:
        return self.value


class UnOpExpr(Expr):
    def __init__(self, op: str, value: Any) -> None:
        self.op = op
        self.value = value

    def __repr__(self) -> str:
        return f"({self.op} {self.value})"


class BinOpExpr(Expr):
    def __init__(self, op: str, left: Any, right: Any) -> None:
        self.op = op
        self.left = left
        self.right = right

    def __repr__(self) -> str:
        return f"({self.left!r} {self.op} {self.right!r})"


class CallExpr(Expr):
    def __init__(self, name: str, *args: Any, schema: Optional[str] = None) -> None:
        self.schema = schema
        self.name = name
        self.args = args

    def __repr__(self) -> str:
        if self.schema:
            f = f"{self.schema}.{self.name}"
        else:
            f = self.name
        return f"{f}({', '.join(map(repr, self.args))})"


class FuncProxy:
    def __init__(self, name: str, *, schema: Optional[str] = None):
        self.schema = schema
        self.name = name

    def __call__(self, *args: Any) -> CallExpr:
        return CallExpr(self.name, *args, schema=self.schema)


class FuncFactory:
    def __init__(self, *, schema: Optional[str] = None):
        self.schema = schema

    def __getattr__(self, name: str) -> FuncProxy:
        return FuncProxy(name, schema=self.schema)


fn = FuncFactory()
