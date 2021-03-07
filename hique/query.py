from __future__ import annotations

from collections import defaultdict
from enum import Enum
from functools import reduce
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from hique.base import FieldExpr, Model
from hique.expr import Expr

if TYPE_CHECKING:
    from hique.engine import Engine

T_Model = TypeVar("T_Model", bound=Model)


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


QualifiedJoinType = Literal[
    JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL
]


class Join:
    @overload
    def __init__(
        self,
        *,
        attr_name: Optional[str],
        dest: Type[Model],
        join_type: Literal[JoinType.CROSS],
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        *,
        attr_name: Optional[str],
        dest: Type[Model],
        join_type: QualifiedJoinType,
        condition: Expr,
    ) -> None:
        ...

    def __init__(
        self,
        *,
        attr_name: Optional[str],
        dest: Type[Model],
        join_type: JoinType,
        condition: Optional[Expr] = None,
    ) -> None:
        self.attr_name = attr_name
        self.dest = dest
        self.join_type = join_type
        self.condition = condition


class Query:
    pass


T_Select = TypeVar("T_Select", bound="BaseSelectQuery")


class BaseSelectQuery(Query):
    def __init__(self, *values: Union[Expr, Type[Model]]) -> None:
        super().__init__()
        self._values: List[Expr] = []
        self._from: List[Any] = []
        self._filter: List[Expr] = []
        self._join: Dict[Type[Model], List[Join]] = defaultdict(list)
        self._join_src: Optional[Type[Model]] = None

        if values:
            self.select(*values)

    def select(self: T_Select, *values: Union[Expr, Type[Model]]) -> T_Select:
        for value in values:
            if isinstance(value, type):
                for field in value.__fields__.keys():
                    self._values.append(getattr(value, field))
            else:
                self._values.append(value)
        return self

    def filter(self: T_Select, *args: Expr) -> T_Select:
        self._filter.extend(args)
        return self

    def switch(self: T_Select, src: Optional[Type[Model]] = None) -> T_Select:
        self._join_src = src if src is not None else self._from[-1]
        return self

    @overload
    def join(
        self: T_Select,
        dest: Type[Model],
        join_type: Literal[JoinType.CROSS],
        *,
        src: Optional[Type[Model]] = None,
    ) -> T_Select:
        ...

    @overload
    def join(
        self: T_Select,
        dest: Type[Model],
        join_type: QualifiedJoinType,
        *,
        condition: Optional[Expr] = None,
        src: Optional[Type[Model]] = None,
    ) -> T_Select:
        ...

    def join(
        self: T_Select,
        dest: Type[Model],
        join_type: JoinType = JoinType.INNER,
        *,
        as_: Optional[str] = None,
        condition: Optional[Expr] = None,
        src: Optional[Type[Model]] = None,
    ) -> T_Select:
        if src is None:
            src = self._join_src
        assert src is not None

        if as_ is None:
            for attr_name, backref in src.__backrefs__.items():
                if issubclass(dest, backref.model):
                    as_ = attr_name
                    break

        if join_type is JoinType.CROSS:
            join = Join(attr_name=as_, dest=dest, join_type=join_type)
        else:
            if condition is None:
                for attr_name, field in dest.__fields__.items():
                    if field.references is not None and issubclass(
                        src, field.references.table
                    ):
                        break
                else:
                    raise RuntimeError(
                        f"Could not find join condition between {src!r} and {dest!r}."
                    )

                condition = getattr(
                    src, field.references.descriptor.attr_name
                ) == getattr(dest, attr_name)
            join = Join(
                attr_name=as_, dest=dest, join_type=join_type, condition=condition
            )

        self._join[src].append(join)
        self._join_src = dest
        return self

    def __repr__(self) -> str:
        if self._filter:
            filter = f" WHERE {reduce(lambda a, i: a & i, self._filter[1:], self._filter[0])}"
        else:
            filter = ""
        return f"SELECT * FROM {','.join(f.__table_name__ for f in self._from)}{filter}"

    def unwrap(self, *, engine: Engine, rows: List[Mapping[str, Any]]) -> List[Any]:
        return rows


class SelectQuery(BaseSelectQuery):
    def __init__(self, *values: Union[Expr, Type[Model]]) -> None:
        super().__init__(*values)

    def from_(self: T_Select, *sources: Type[Model], replace: bool = False) -> T_Select:
        if replace:
            self._from.clear()
            self._join.clear()
            self._join_src = None
        self._from.extend(sources)
        self._join_src = self._from[-1]
        return self


def unwrap_models(
    *,
    engine: Engine,
    source: Type[T_Model],
    joins: Mapping[Type[Model], List[Join]],
    rows: List[Mapping[str, Any]],
) -> List[T_Model]:
    result: List[T_Model] = []

    def get_pk_fields(model: Type[Model]) -> List[str]:
        return [
            f"{model.__alias__}.{field.attr_name}"
            for field in model.__fields__.values()
            if field.primary_key
        ]

    models: Dict[Type[Model], List[str]] = {
        source: get_pk_fields(source),
        **{
            _join.dest: get_pk_fields(_join.dest)
            for _joins in joins.values()
            for _join in _joins
        },
    }

    def get_pk_from_row(model: Type[Model], row: Mapping[str, Any]) -> Tuple[Any, ...]:
        return tuple(row.get(pk_field) for pk_field in models[model])

    joins_by_dest: Dict[Type[Model], List[Tuple[Type[Model], Join]]] = {}
    for src, _joins in joins.items():
        for _join in _joins:
            if _join.attr_name:
                joins_by_dest.setdefault(_join.dest, []).append((src, _join))

    store: Dict[Type[Model], Dict[Tuple[Any, ...], Model]] = {
        model: {} for model in models
    }

    for row in rows:
        objects = {}

        for model in models:
            pk = get_pk_from_row(model, row)
            instance = store[model].get(pk)
            if instance is None:
                instance = store[model][pk] = model(__engine__=engine)
                if model is source:
                    result.append(cast(T_Model, instance))
            objects[model.__alias__] = instance

        for key, value in row.items():
            parts = key.split(".", 1)
            if len(parts) == 2:
                instance = objects.get(parts[0])
                if instance is not None:
                    setattr(instance, parts[1], value)

        for instance in objects.values():
            for src, _join in joins_by_dest.get(type(instance), ()):
                if _join.attr_name in src.__backrefs__.keys() | src.__fields__.keys():
                    objects[src.__alias__].__data__.setdefault(
                        _join.attr_name, []
                    ).append(instance)
                elif _join.attr_name:
                    if not hasattr(objects[src.__alias__], _join.attr_name):
                        setattr(objects[src.__alias__], _join.attr_name, [])
                    getattr(objects[src.__alias__], _join.attr_name).append(instance)

    return result


class ModelSelectQuery(Generic[T_Model], BaseSelectQuery):
    @overload
    def __init__(
        self, model: Type[T_Model], /, *values: Union[Type[Model], Expr]
    ) -> None:
        ...

    @overload
    def __init__(self, *values: Union[Type[Model], Expr]) -> None:
        ...

    def __init__(self, *values: Union[Type[Model], Expr]) -> None:
        super().__init__(*values)

        def find_model(values: Any) -> Optional[Type[Model]]:
            for value in values:
                if isinstance(value, FieldExpr):
                    return value.table
                elif isinstance(value, Expr):
                    model = find_model(value.args)
                    if model is not None:
                        return model
            return None

        model = find_model(self._values)
        if model is None:
            raise RuntimeError("Cannot construct ModelSelectQuery without a model.")
        self._from.append(model)
        self._join_src = model

    def unwrap(self, *, engine: Engine, rows: List[Mapping[str, Any]]) -> List[T_Model]:
        return unwrap_models(
            engine=engine, source=self._from[0], joins=self._join, rows=rows
        )


class InsertQuery(Generic[T_Model], Query):
    def __init__(self, model: Type[T_Model], **values: Any) -> None:
        self._model = model
        self._values: Dict[str, Any] = values.copy()
        self._returning: List[Expr] = []

    def returning(self, *exprs: Any) -> InsertQuery[T_Model]:
        for expr in exprs:
            if issubclass(expr, Model):
                self._returning.extend(
                    getattr(expr, attr_name) for attr_name in expr.__fields__.keys()
                )
            else:
                self._returning.append(expr)
        return self

    def unwrap(self, *, engine: Engine, rows: List[Mapping[str, Any]]) -> List[T_Model]:
        return unwrap_models(engine=engine, source=self._model, joins={}, rows=rows)
