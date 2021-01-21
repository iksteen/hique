from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence, Type

if TYPE_CHECKING:
    from hique.base import Model


class Expr:
    op: str = ""
    args: Sequence[Any] = ()
    __alias__: Optional[str] = None

    def __init__(self, op: str, *args: Any) -> None:
        self.op = op
        self.args = args

    def alias(self, alias: str, *, table: Optional[Type[Model]] = None) -> Expr:
        if table is not None:
            self.__alias__ = f"{table.__alias__}.{alias}"
        else:
            self.__alias__ = alias
        return self

    def __lt__(self, other: Any) -> Expr:
        return Expr("lt", self, other)

    def __le__(self, other: Any) -> Expr:
        return Expr("le", self, other)

    def __eq__(self, other: Any) -> Expr:  # type: ignore
        return Expr("eq", self, other)

    def __ne__(self, other: Any) -> Expr:  # type: ignore
        return Expr("ne", self, other)

    def __gt__(self, other: Any) -> Expr:
        return Expr("gt", self, other)

    def __ge__(self, other: Any) -> Expr:
        return Expr("ge", self, other)

    def __neg__(self) -> Expr:
        return Expr("neg", self)

    def __pos__(self) -> Expr:
        return Expr("pos", self)

    def __abs__(self) -> Expr:
        return Expr("abs", self)

    def __invert__(self) -> Expr:
        return Expr("invert", self)

    def __add__(self, other: Any) -> Expr:
        return Expr("add", self, other)

    def __sub__(self, other: Any) -> Expr:
        return Expr("sub", self, other)

    def __mul__(self, other: Any) -> Expr:
        return Expr("mul", self, other)

    def __matmul__(self, other: Any) -> Expr:
        return Expr("matmul", self, other)

    def __truediv__(self, other: Any) -> Expr:
        return Expr("div", self, other)

    def __floordiv__(self, other: Any) -> Expr:
        return Expr("floordiv", self, other)

    def __mod__(self, other: Any) -> Expr:
        return Expr("mod", self, other)

    def __divmod__(self, other: Any) -> Expr:
        return Expr("divmod", self, other)

    def __pow__(self, other: Any, modulo: Any = None) -> Expr:
        return Expr("pow", self, other, modulo)

    def __lshift__(self, other: Any) -> Expr:
        return Expr("lshift", self, other)

    def __rshift__(self, other: Any) -> Expr:
        return Expr("rshift", self, other)

    def __and__(self, other: Any) -> Expr:
        return Expr("and", self, other)

    def __xor__(self, other: Any) -> Expr:
        return Expr("xor", self, other)

    def __or__(self, other: Any) -> Expr:
        return Expr("or", self, other)

    def __radd__(self, other: Any) -> Expr:
        return Expr("add", other, self)

    def __rsub__(self, other: Any) -> Expr:
        return Expr("sub", other, self)

    def __rmul__(self, other: Any) -> Expr:
        return Expr("mul", other, self)

    def __rmatmul__(self, other: Any) -> Expr:
        return Expr("matmul", other, self)

    def __rtruediv__(self, other: Any) -> Expr:
        return Expr("div", other, self)

    def __rfloordiv__(self, other: Any) -> Expr:
        return Expr("floordiv", other, self)

    def __rmod__(self, other: Any) -> Expr:
        return Expr("mod", other, self)

    def __rdivmod__(self, other: Any) -> Expr:
        return Expr("divmod", other, self)

    def __rpow__(self, other: Any) -> Expr:
        return Expr("pow", other, self, None)

    def __rlshift__(self, other: Any) -> Expr:
        return Expr("lshift", other, self)

    def __rrshift__(self, other: Any) -> Expr:
        return Expr("rshift", other, self)

    def __rand__(self, other: Any) -> Expr:
        return Expr("and", other, self)

    def __rxor__(self, other: Any) -> Expr:
        return Expr("xor", other, self)

    def __ror__(self, other: Any) -> Expr:
        return Expr("or", other, self)

    def __round__(self, ndigits: Optional[int] = None) -> Expr:
        if ndigits is None:
            return Expr("round", self)
        else:
            return Expr("round", self, ndigits)

    def __trunc__(self) -> Expr:
        return Expr("trunc", self)

    def __floor__(self) -> Expr:
        return Expr("floor", self)

    def __ceil__(self) -> Expr:
        return Expr("ceil", self)

    def is_null(self) -> Expr:
        return Expr("is_null", self)

    def is_not_null(self) -> Expr:
        return Expr("is_not_null", self)

    def __repr__(self) -> str:
        return f"{self.op}({', '.join(map(repr, self.args))})"


class Literal(Expr):
    def __init__(self, value: str) -> None:
        super().__init__("literal", value)

    def __repr__(self) -> str:
        return str(self.args[0])


class CallExpr(Expr):
    def __init__(self, name: str, *args: Any, schema: Optional[str] = None) -> None:
        super().__init__("call", args)
        self.schema = schema
        self.name = name

    def __repr__(self) -> str:
        if self.schema is not None:
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
