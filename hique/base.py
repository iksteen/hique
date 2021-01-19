from __future__ import annotations

from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)
from weakref import WeakKeyDictionary

from hique.expr import Expr


class BaseMeta(type):
    def __new__(
        mcs: Type[BaseMeta], name: str, bases: Tuple[type, ...], attr: Dict[str, Any]
    ) -> BaseMeta:
        # Set up default table name.
        if "__table_name__" not in attr:
            attr["__table_name__"] = name.lower()

        # Clone fields so fields of the subclass don't end up in the superclass.
        _fields = {}
        for base in bases:
            if issubclass(base, Base):
                _fields.update(base.__fields__)
        attr["__fields__"] = _fields

        return cast(BaseMeta, super(BaseMeta, mcs).__new__(mcs, name, bases, attr))


class Base(metaclass=BaseMeta):
    __table_name__: ClassVar[str]
    __alias__: ClassVar[Optional[str]] = None
    __fields__: ClassVar[Dict[str, FieldAttr[Any]]] = {}

    __data__: Dict[str, Any]

    def __init__(self, **kwargs: Any):
        self.__data__ = {}

        cls = type(self)

        for key, value in kwargs.items():
            field_impl = getattr(cls, key, None)
            if not isinstance(field_impl, FieldAttrDescriptor):
                raise KeyError(f"{key} is not a field")
            setattr(self, key, value)

    def __str__(self) -> str:
        return self.__alias__ or self.__table_name__

    def __repr__(self) -> str:
        return self.__alias__ or self.__table_name__


def alias(cls: Type[Base], name: str) -> Type[Base]:
    return type(
        f"{cls.__name__}:{name}",
        (cls,),
        {"__table_name__": cls.__table_name__, "__alias__": name},
    )


T = TypeVar("T")


class FieldAttrDescriptor(Generic[T], Expr):
    def __init__(self, table: Type[Base], field: FieldAttr[Any]) -> None:
        self.table = table
        self.field = field

    def __str__(self) -> str:
        return self.field.name

    def __repr__(self) -> str:
        return f"{self.table()!r}.{self.field.name}"


NO_VALUE = object()


class FieldAttr(Generic[T]):
    impl: WeakKeyDictionary[Type[Base], FieldAttrDescriptor[T]]
    name: str
    attr_name: str

    def __init__(
        self,
        *,
        default: Optional[Callable[[], T]] = None,
        name: Optional[str] = None,
    ):
        self.impl = WeakKeyDictionary()

        if default is not None:
            setattr(self, "default", default)

        if name is not None:
            self.name = name

    def default(self) -> T:
        raise NotImplementedError

    def get_impl(self, owner: Type[Base]) -> FieldAttrDescriptor[T]:
        impl = self.impl.get(owner)
        if impl is None:
            impl = self.impl[owner] = FieldAttrDescriptor(owner, self)
        return impl

    def __set_name__(self, owner: Type[Base], name: str) -> None:
        owner.__fields__[name] = self
        self.attr_name = name
        if not hasattr(self, "name"):
            self.name = name

    @overload
    def __get__(self, inst: Base, owner: Type[Base]) -> T:
        ...

    @overload
    def __get__(self, inst: None, owner: Type[Base]) -> FieldAttrDescriptor[T]:
        ...

    def __get__(
        self, inst: Optional[Base], owner: Type[Base]
    ) -> Union[None, T, FieldAttrDescriptor[T]]:
        impl = self.get_impl(owner)
        if inst is None:
            return impl

        value = inst.__data__.get(self.name, NO_VALUE)
        if value is not NO_VALUE:
            return cast(Optional[T], value)

        value = inst.__data__[self.name] = self.default()
        return cast(Optional[T], value)

    def __set__(self, inst: Base, value: T) -> None:
        inst.__data__[self.name] = value


class NullableFieldAttr(FieldAttr[Optional[T]]):
    def default(self) -> Optional[T]:
        return None
