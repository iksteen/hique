from __future__ import annotations

from typing import ClassVar, Optional, Dict, Any, Type, TypeVar, Generic, Callable, overload, Union, cast, Tuple
from weakref import WeakKeyDictionary

from hique.expr import Expr


class BaseMeta(type):
    def __new__(mcs: Type[BaseMeta], name: str, bases: Tuple[type, ...], attr: Dict[str, Any]) -> BaseMeta:
        # Set up default table name.
        if "__table_name__" not in attr:
            attr["__table_name__"] = name.lower()

        # Clone fields so fields of the subclass don't end up in the superclass.
        _fields = {}
        for base in bases:
            if issubclass(base, Base):
                _fields.update(base._fields)
        attr["_fields"] = _fields

        return cast(BaseMeta, super(BaseMeta, mcs).__new__(mcs, name, bases, attr))


class Base(metaclass=BaseMeta):
    __table_name__: ClassVar[str]
    _alias: ClassVar[Optional[str]] = None
    _fields: ClassVar[Dict[str, FieldAttr[Any, Any]]] = {}

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


def alias(cls: Type[Base], name: str) -> Type[Base]:
    return type(f"{cls.__name__}:{name}", (cls,), {"__table_name__": cls.__table_name__, "_alias": name})


T = TypeVar("T")


class FieldImplBase(Generic[T], Expr):
    def __init__(self, table: Type[Base], field: FieldAttr[Any, Any]) -> None:
        self.table = table
        self.field = field

    def from_python(self, value: Any) -> Optional[T]:
        raise NotImplementedError

    def to_python(self, value: Optional[T]) -> Any:
        raise NotImplementedError

    def __str__(self) -> str:
        return self.field.name

    def __repr__(self) -> str:
        return f'{self.table()!r}.{self.field.name}'


V = TypeVar("V")
I = TypeVar("I", bound=FieldImplBase[Any])
NO_VALUE = object()


class FieldAttr(Generic[V, I]):
    impl_type: Type[I]
    impl: WeakKeyDictionary[Type[Base], I]
    nullable = False
    default: Optional[Callable[[], V]]
    name: str
    attr_name: str

    def __init__(self, *, default: Optional[Callable[[], V]] = None, name: Optional[str] = None):
        self.impl = WeakKeyDictionary()
        self.default = default
        if name is not None:
            self.name = name

    def get_impl(self, owner: Type[Base]) -> I:
        impl = self.impl.get(owner)
        if impl is None:
            impl = self.impl[owner] = self.impl_type(owner, self)
        return impl

    def __set_name__(self, owner: Type[Base], name: str) -> None:
        owner._fields[name] = self
        self.attr_name = name
        if not hasattr(self, "name"):
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
