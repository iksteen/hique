from __future__ import annotations

import asyncio
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)
from weakref import WeakKeyDictionary

from hique.base import Model
from hique.query import ModelSelectQuery

if TYPE_CHECKING:
    from hique.engine import Engine

T_Model = TypeVar("T_Model", bound=Model)


class BackrefAttr(Generic[T_Model]):
    def __init__(
        self, descriptor: Backref[T_Model], inst: Model, model: Type[T_Model]
    ) -> None:
        self.descriptor = descriptor
        self.inst = inst
        self.model = model
        self.attr_name = descriptor.attr_name

        if descriptor.ref_attr is None:
            for m_attr_name, m_descriptor in model.__fields__.items():
                if m_descriptor.references is not None and isinstance(
                    inst, m_descriptor.references.table
                ):
                    break
            else:
                raise RuntimeError(
                    f"Could not find relationship between {self.inst} and {self.model}."
                )
        else:
            m_attr_name = descriptor.ref_attr
            m_descriptor = self.model.__fields__[m_attr_name]
            assert m_descriptor.references is not None and isinstance(
                inst, m_descriptor.references.table
            )

        self.model_attr = getattr(self.model, m_attr_name)
        self.inst_attr_name = m_descriptor.references.descriptor.attr_name

    def __str__(self) -> str:
        return self.attr_name

    def __repr__(self) -> str:
        return f"{self.model}.{self.attr_name}"

    def clear(self) -> None:
        self.inst.__data__.pop(self.attr_name, None)

    @property
    def query(self) -> ModelSelectQuery[T_Model]:
        return ModelSelectQuery(self.model).filter(
            self.model_attr == getattr(self.inst, self.inst_attr_name)
        )

    async def __call__(
        self, *, engine: Optional[Engine] = None, refresh: bool = False
    ) -> List[T_Model]:
        if engine is None:
            engine = self.inst.__engine__
            if engine is None:
                raise RuntimeError("Model not instantiated from engine.")

        value = self.inst.__data__.get(self.attr_name)
        if not refresh and value is not None:
            if isinstance(value, list):
                return cast(List[T_Model], value)
            return cast(List[T_Model], await value)

        loop = asyncio.get_event_loop()
        f = self.inst.__data__[self.attr_name] = loop.create_future()
        try:
            result = await engine.execute(self.query)
            f.set_result(result)
            return result
        except Exception as e:
            del self.inst.__data__[self.attr_name]
            raise e from None

    def __await__(self) -> Generator[Any, None, List[T_Model]]:
        return (yield from self.__call__().__await__())


class Backref(Generic[T_Model]):
    instances: WeakKeyDictionary[Model, BackrefAttr[T_Model]]
    attr_name: str

    def __init__(
        self,
        model: Union[Type[T_Model], Callable[[], Type[T_Model]]],
        *,
        attr: Optional[str] = None,
    ):
        self._model = model
        self.ref_attr = attr
        self.instances = WeakKeyDictionary()

    @property
    def model(self) -> Type[T_Model]:
        if isinstance(self._model, type):
            return self._model
        self._model = self._model()
        return self._model

    def __set_name__(self, owner: Type[Model], name: str) -> None:
        owner.__backrefs__[name] = self
        self.attr_name = name

    @overload
    def __get__(self, inst: Model, owner: Type[Model]) -> BackrefAttr[T_Model]:
        ...

    @overload
    def __get__(self, inst: None, owner: Type[Model]) -> Backref[T_Model]:
        ...

    def __get__(
        self, inst: Optional[Model], owner: Type[Model]
    ) -> Union[BackrefAttr[T_Model], Backref[T_Model]]:
        if inst is None:
            return self

        attr = self.instances.get(inst)
        if attr is None:
            attr = self.instances[inst] = BackrefAttr(self, inst, self.model)
        return attr

    def __set__(self, instance: Model, value: List[T_Model]) -> None:
        instance.__data__[self.attr_name] = value
