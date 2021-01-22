from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from types import TracebackType
from typing import (
    Any,
    Awaitable,
    Deque,
    Generator,
    List,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
    overload,
)

from hique.database import Connection, Database, Transaction
from hique.query import Query
from hique.task_local_state import TaskLocalState

T = TypeVar("T")


@dataclass
class TransactionState:
    @classmethod
    def new(cls) -> TransactionState:
        return cls(lock=asyncio.Lock(), stack=deque())

    lock: asyncio.Lock
    stack: Deque[TransactionContext]
    borrows: int = 0
    inherited: bool = False
    connection: Optional[Connection] = None


class TransactionContext:
    _connection: Optional[Connection]
    _transaction: Optional[Transaction]

    def __init__(
        self, database: Database, state: TaskLocalState[TransactionState]
    ) -> None:
        self._database = database
        self._state = state
        self._completed = False
        self._transaction = None

    @property
    def state(self) -> TransactionState:
        return self._state.current

    def __await__(self) -> Generator[Any, None, TransactionState]:
        conn = self.state.connection
        if conn is None:
            conn = yield from self._database.connection().__await__()
            self._connection = self.state.connection = conn

        self._transaction = yield from conn.transaction().__await__()
        self.state.stack.append(self)
        return self.state

    def _check_state(self) -> None:
        if self.state.borrows:
            raise RuntimeError(
                "Can't release a transaction that is currently borrowed."
            )

        if self is not self.state.stack[-1]:
            raise RuntimeError("Releasing transaction in incorrect order.")

        if self._connection is not self.state.connection:
            raise RuntimeError("Task switched when releasing transaction.")

    async def _pop_transaction(self) -> None:
        if self._connection is None:
            raise RuntimeError("Transaction has invalid state.")

        self._completed = True
        self.state.stack.pop()

        if not self.state.stack:
            try:
                async with self.state.lock:
                    await self._connection.release()
            finally:
                self.state.connection = None

    async def commit(self) -> None:
        if self._transaction is None:
            raise RuntimeError("Transaction has invalid state.")

        self._check_state()
        try:
            async with self.state.lock:
                await self._transaction.commit()
        finally:
            await self._pop_transaction()

    async def rollback(self) -> None:
        if self._transaction is None:
            raise RuntimeError("Transaction has invalid state.")

        self._check_state()
        try:
            async with self.state.lock:
                await self._transaction.rollback()
        finally:
            await self._pop_transaction()

    async def __aenter__(self) -> TransactionState:
        return await self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if self._transaction is None:
            raise RuntimeError("Transaction has invalid state.")

        self._check_state()
        try:
            if not self._completed:
                async with self.state.lock:
                    if exc_type is not None:
                        await self._transaction.rollback()
                    else:
                        await self._transaction.commit()
        finally:
            await self._pop_transaction()


class Engine:
    def __init__(self, database: Database) -> None:
        self.database = database
        self._state = TaskLocalState(default=TransactionState.new)

    @property
    def state(self) -> TransactionState:
        return self._state.current

    @overload
    async def execute(self, query: Query) -> List[Mapping[str, Any]]:
        ...

    @overload
    async def execute(self, query: str, *args: Any) -> List[Mapping[str, Any]]:
        ...

    async def execute(
        self, query: Union[Query, str], *args: Any
    ) -> List[Mapping[str, Any]]:
        if isinstance(query, Query):
            query_, args = self.database.query_builder(query)
        else:
            query_ = query

        conn = self.state.connection
        if conn is None:
            conn = await self.database.connection()
            release = True
        else:
            release = False

        try:
            async with self.state.lock:
                result = await conn.execute(query_, *args)
                return result
        finally:
            if release:
                await conn.release()

    def transaction(self) -> TransactionContext:
        if self.state.inherited:
            raise RuntimeError("Cannot start new transaction with inherited context.")

        if self.state.borrows:
            raise RuntimeError("Can't start a new transaction while it is borrowed.")

        return TransactionContext(self.database, self._state)

    async def in_context(self, state: TransactionState, f: Awaitable[T]) -> T:
        if self.state.connection is not None:
            raise RuntimeError(
                "Cannot inherit context when current state is in transaction."
            )

        self.state.lock = state.lock
        self.state.inherited = True
        self.state.connection = state.connection
        self.state.stack = state.stack
        state.borrows += 1
        try:
            return await f
        finally:
            state.borrows -= 1
