from __future__ import annotations

from functools import reduce
from typing import Annotated, ClassVar, Union, overload, Type, Generic, TypeVar, Optional, Callable, Any, Dict, List, cast
from weakref import WeakKeyDictionary


class Expr:
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


class Query:
    pass


class SelectQuery(Query):
    def __init__(self) -> None:
        super().__init__()
        self._from: List[Any] = []
        self._filter: List[Expr] = []

    def from_(self, *sources: Any, replace: bool = False) -> SelectQuery:
        if replace:
            self._from.clear()
        self._from.extend(sources)
        return self

    def filter(self, *args: Expr, **kwargs: Any) -> SelectQuery:
        self._filter.extend(args)
        self._filter.extend(
            getattr(self._from[0], key) == value
            for key, value in kwargs.items()
        )
        return self

    def __repr__(self) -> str:
        filter = reduce(lambda a, i: a & i, self._filter[1:], self._filter[0])
        return f"SELECT * FROM {','.join(f._table_name for f in self._from)} WHERE {filter}"


class Base:
    __table_name__: ClassVar[Optional[str]] = None
    _alias: ClassVar[Optional[str]] = None

    _data: Dict[str, Any]

    def __init__(self, **kwargs: Any):
        self._data = {}

        cls = type(self)

        for key, value in kwargs.items():
            field_impl = getattr(cls, key, None)
            if not isinstance(field_impl, FieldImplBase):
                raise KeyError(f"{key} is not a field")
            setattr(self, key, value)

    @classmethod
    @property
    def _table_name(cls) -> str:
        if cls._alias is not None:
            return cls._alias
        elif cls.__table_name__ is not None:
            return cls.__table_name__
        else:
            return cls.__name__.lower()

    def __str__(self) -> str:
        return self._table_name

    def __repr__(self) -> str:
        return self._table_name


def alias(cls: Type[T], name: str) -> Type[T]:
    return type(f"{cls.__name__}:{name}", (cls,), {"_alias": name})


def select(cls: Type[Base], *fields: Any) -> SelectQuery:
    return SelectQuery(*fields).from_(cls)


T = TypeVar("T")
class FieldImplBase(Generic[T], Expr):
    def __init__(self, table: Type[Base], name: str) -> None:
        self.table = table
        self.name = name

    def from_python(self, value: Any) -> Optional[T]:
        ...

    def to_python(self, value: Optional[T]) -> Any:
        ...

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f'{self.table()!r}.{self.name}'


V = TypeVar("V")
I = TypeVar("I", bound=FieldImplBase[Any])
NO_VALUE = object()


class FieldAttr(Generic[V, I]):
    impl_type: Type[I]
    impl: WeakKeyDictionary[Type[Base], I]
    nullable = False
    default: Optional[Callable[[], V]]

    def __init__(self, default: Optional[Callable[[], V]] = None):
        self.impl = WeakKeyDictionary()
        self.default = default

    def get_impl(self, owner: Type[Base]) -> I:
        impl = self.impl.get(owner)
        if impl is None:
            impl = self.impl[owner] = self.impl_type(owner, self.name)
        return impl

    def __set_name__(self, owner: Type[Base], name: str) -> None:
        self.name = name

    @overload
    def __get__(self, inst: Base, owner: Type[Base]) -> V:
        ...

    @overload
    def __get__(self, inst: None, owner: Type[Base]) -> I:
        ...

    def __get__(self, inst: Optional[Base], owner: Type[Base]) -> Union[None, V, I]:
        impl = self.get_impl(owner)
        if inst is None:
            return impl

        value = inst._data.get(self.name, NO_VALUE)
        if value is not NO_VALUE:
            return cast(Optional[V], impl.to_python(value))

        if self.default is not None:
            value = self.default()
            self.__set__(inst, value)
            return cast(Optional[V], impl.from_python(inst._data[self.name]))

        if self.nullable:
            return None
        raise NotImplementedError

    def __set__(self, inst: Base, value: V) -> None:
        if self.nullable and value is None:
            inst._data[self.name] = value
        else:
            impl = self.get_impl(type(inst))
            inst._data[self.name] = impl.from_python(value)


class NullableFieldAttr(FieldAttr[Optional[V], I]):
    nullable = True


class TextFieldImpl(FieldImplBase[str]):
    @staticmethod
    def from_python(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            raise TypeError(f"Expected string, got {type(value).__name__}.")
        return value

    @staticmethod
    def to_python(value: Optional[str]) -> Optional[str]:
        return value

    @staticmethod
    def contains(something: str) -> Expr:
        return Expr()


class TextField(FieldAttr[str, TextFieldImpl]):
    impl_type = TextFieldImpl


class NullableTextField(NullableFieldAttr[str, TextFieldImpl], TextField):
    pass


class Foo(Base):
    name = TextField()
    email = NullableTextField()
    zmail = NullableTextField()


class Bar(Foo):
    pass


z = Foo(name="Base")
print(select(Foo).filter((Foo.name == "Ingmar Steen") | (Foo.email == "i.steen@nrc.nl"), Foo.zmail.is_null()))
z.email = "bar"
z.email = None
z.name = "foo"
z.name = None
