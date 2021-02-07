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


class ModelMeta(type):
    def __new__(
        mcs: Type[ModelMeta], name: str, bases: Tuple[type, ...], attr: Dict[str, Any]
    ) -> ModelMeta:
        # Clone fields so fields of the subclass don't end up in the superclass.
        _fields = {}
        # If all parents are abstract, come up with a catchy table name. Otherwise,
        # inherit the table name from the parent.
        parent_is_abstract = True
        for base in bases[::-1]:
            if issubclass(base, Model):
                _fields.update(base.__fields__)
                if not base.__abstract__:
                    parent_is_abstract = False
        attr["__fields__"] = _fields

        # Set up default table name.
        if "__abstract__" not in attr:
            attr["__abstract__"] = False

        if parent_is_abstract:
            if "__table_name__" not in attr:
                attr["__table_name__"] = name.lower()

            if "__alias__" not in attr:
                attr["__alias__"] = attr["__table_name__"]
        else:
            if "__alias__" not in attr:
                attr["__alias__"] = name.lower()

        return cast(ModelMeta, super(ModelMeta, mcs).__new__(mcs, name, bases, attr))


class Model(metaclass=ModelMeta):
    __abstract__: ClassVar[bool] = True
    __table_name__: ClassVar[str]
    __alias__: ClassVar[str]
    __fields__: ClassVar[Dict[str, Field[Any]]] = {}

    __data__: Dict[str, Any]

    def __init__(self, **kwargs: Any):
        self.__data__ = {}

        cls = type(self)

        for key, value in kwargs.items():
            descriptor = getattr(cls, key, None)
            if not isinstance(descriptor, FieldExpr):
                raise KeyError(f"{key} is not a field")
            setattr(self, key, value)

    def __str__(self) -> str:
        return self.__alias__

    def __repr__(self) -> str:
        return self.__alias__


T_Model = TypeVar("T_Model", bound=Model)


def alias(cls: Type[T_Model], name: str) -> Type[T_Model]:
    return type(
        f"{cls.__name__}:{name}",
        (cls,),
        {"__table_name__": cls.__table_name__, "__alias__": name},
    )


T = TypeVar("T")


class FieldExpr(Generic[T], Expr):
    def __init__(self, table: Type[Model], descriptor: Field[T]) -> None:
        super().__init__("field")
        self.table = table
        self.descriptor = descriptor

    def __str__(self) -> str:
        return self.descriptor.name

    def __repr__(self) -> str:
        return f"{self.table()!r}.{self.descriptor.name}"


MISSING = object()


class Field(Generic[T]):
    field_exprs: WeakKeyDictionary[Type[Model], FieldExpr[T]]
    name: str
    attr_name: str
    primary_key: bool
    expr: Optional[Callable[[], Expr]]

    @overload
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        default: Optional[Callable[[], T]] = None,
        primary_key: bool = False,
        references: Optional[FieldExpr[T]] = None,
    ):
        ...

    @overload
    def __init__(self, *, expr: Callable[[], Expr]):
        ...

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        default: Optional[Callable[[], T]] = None,
        primary_key: bool = False,
        references: Optional[FieldExpr[T]] = None,
        expr: Optional[Callable[[], Expr]] = None,
    ):
        self.field_exprs = WeakKeyDictionary()

        if name is not None:
            self.name = name

        if default is not None:
            setattr(self, "default", default)

        self.primary_key = primary_key
        self.references = references
        self.expr = expr

    def default(self) -> T:
        raise NotImplementedError

    def __set_name__(self, owner: Type[Model], name: str) -> None:
        owner.__fields__[name] = self
        self.attr_name = name
        if not hasattr(self, "name"):
            self.name = name

    @overload
    def __get__(self, inst: Model, owner: Type[Model]) -> T:
        ...

    @overload
    def __get__(self, inst: None, owner: Type[Model]) -> FieldExpr[T]:
        ...

    def __get__(
        self, inst: Optional[Model], owner: Type[Model]
    ) -> Union[T, FieldExpr[T]]:
        if inst is None:
            field_expr = self.field_exprs.get(owner)
            if field_expr is None:
                field_expr = self.field_exprs[owner] = FieldExpr(owner, self)
            return field_expr

        value = inst.__data__.get(self.attr_name, MISSING)
        if value is not MISSING:
            return cast(T, value)

        value = inst.__data__[self.attr_name] = self.default()
        return cast(T, value)

    def __set__(self, inst: Model, value: T) -> None:
        inst.__data__[self.attr_name] = value


class NullableField(Field[Optional[T]]):
    def default(self) -> None:
        return None
