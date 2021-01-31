from __future__ import annotations

from collections import defaultdict
from enum import Enum
from functools import reduce
from typing import Any, Dict, List, Literal, Optional, Type, Union, overload

from hique.base import Model
from hique.expr import Expr


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
        self, *, dest: Type[Model], join_type: Literal[JoinType.CROSS]
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        *,
        dest: Type[Model],
        join_type: QualifiedJoinType,
        condition: Expr,
    ) -> None:
        ...

    def __init__(
        self,
        *,
        dest: Type[Model],
        join_type: JoinType,
        condition: Optional[Expr] = None,
    ) -> None:
        self.dest = dest
        self.join_type = join_type
        self.condition = condition


class Query:
    pass


class SelectQuery(Query):
    def __init__(self, *values: Union[Expr, Type[Model]]) -> None:
        super().__init__()
        self._values: List[Expr] = []
        self._from: List[Any] = []
        self._filter: List[Expr] = []
        self._join: Dict[Type[Model], List[Join]] = defaultdict(list)
        self._join_src: Optional[Type[Model]] = None

        if values:
            self.select(*values)

    def select(self, *values: Union[Expr, Type[Model]]) -> SelectQuery:
        for value in values:
            if isinstance(value, type):
                for field in value.__fields__.keys():
                    self._values.append(getattr(value, field))
            else:
                self._values.append(value)
        return self

    def from_(self, *sources: Type[Model], replace: bool = False) -> SelectQuery:
        if replace:
            self._from.clear()
            self._join.clear()
            self._join_src = None
        self._from.extend(sources)
        self._join_src = self._from[-1]
        return self

    def filter(self, *args: Expr) -> SelectQuery:
        self._filter.extend(args)
        return self

    def switch(self, src: Optional[Type[Model]] = None) -> SelectQuery:
        self._join_src = src if src is not None else self._from[-1]
        return self

    @overload
    def join(
        self,
        dest: Type[Model],
        join_type: Literal[JoinType.CROSS],
        *,
        src: Optional[Type[Model]] = None,
    ) -> SelectQuery:
        ...

    @overload
    def join(
        self,
        dest: Type[Model],
        join_type: QualifiedJoinType,
        *,
        condition: Optional[Expr] = None,
        src: Optional[Type[Model]] = None,
    ) -> SelectQuery:
        ...

    def join(
        self,
        dest: Type[Model],
        join_type: JoinType = JoinType.INNER,
        *,
        condition: Optional[Expr] = None,
        src: Optional[Type[Model]] = None,
    ) -> SelectQuery:
        if src is None:
            src = self._join_src
        assert src is not None

        if join_type is JoinType.CROSS:
            join = Join(dest=dest, join_type=join_type)
        else:
            if condition is None:
                for attr_name, field in dest.__fields__.items():
                    if (
                        field.references is not None
                        and field.references.table is self._join_src
                    ):
                        break
                else:
                    raise RuntimeError(
                        f"Could not find join condition between {self._join_src!r} and {dest!r}."
                    )
                condition = field.references == getattr(dest, attr_name)
            join = Join(dest=dest, join_type=join_type, condition=condition)

        self._join[src].append(join)
        self._join_src = dest
        return self

    def __repr__(self) -> str:
        if self._filter:
            filter = f" WHERE {reduce(lambda a, i: a & i, self._filter[1:], self._filter[0])}"
        else:
            filter = ""
        return f"SELECT * FROM {','.join(f.__table_name__ for f in self._from)}{filter}"
