from __future__ import annotations

from contextvars import ContextVar, Token
from types import TracebackType
from typing import Any, Generator, List, Mapping, Optional, Type, Union, overload

from hique.database import Connection, Database, Transaction
from hique.query import Query


class TransactionContext:
    _connection: Optional[Connection]
    _transaction: Optional[Transaction]
    _prev_transaction: Optional[Token[Optional[TransactionContext]]]

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._completed = False
        self._connection = None
        self._transaction = None
        self._prev_transaction = None

    def __await__(self) -> Generator[Any, None, None]:
        conn = self._engine._connection.get()
        if conn is None:
            conn = yield from self._engine.database.connection().__await__()
            self._engine._connection.set(conn)
        self._connection = conn

        self._transaction = yield from conn.transaction().__await__()
        self._prev_transaction = self._engine._transaction.set(self)
        self._engine._tr_depth.set(self._engine._tr_depth.get() + 1)

    def _check_state(self) -> None:
        if self is not self._engine._transaction.get():
            raise RuntimeError("Releasing transaction in incorrect order.")

        if self._connection is not self._engine._connection.get():
            raise RuntimeError("Task switched when releasing transaction.")

    async def _pop_transaction(self) -> None:
        if self._prev_transaction is None or self._connection is None:
            raise RuntimeError("Transaction has invalid state.")

        self._completed = True
        self._engine._transaction.reset(self._prev_transaction)
        self._engine._tr_depth.set(self._engine._tr_depth.get() - 1)
        if self._engine._tr_depth.get() == 0:
            try:
                await self._connection.release()
            finally:
                self._engine._connection.set(None)

    async def commit(self) -> None:
        if self._transaction is None:
            raise RuntimeError("Transaction has invalid state.")

        self._check_state()
        try:
            await self._transaction.commit()
        finally:
            await self._pop_transaction()

    async def rollback(self) -> None:
        if self._transaction is None:
            raise RuntimeError("Transaction has invalid state.")

        self._check_state()
        try:
            await self._transaction.rollback()
        finally:
            await self._pop_transaction()

    async def __aenter__(self) -> None:
        await self

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
                if exc_type is not None:
                    await self._transaction.rollback()
                else:
                    await self._transaction.commit()
        finally:
            await self._pop_transaction()


class Engine:
    def __init__(self, database: Database) -> None:
        self.database = database
        self._connection = ContextVar[Optional[Connection]]("_connection", default=None)
        self._tr_depth = ContextVar[int]("_tr_depth", default=0)
        self._transaction = ContextVar[Optional[TransactionContext]](
            "_transaction", default=None
        )

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
            query, args = self.database.query_builder(query)

        conn = self._connection.get()
        if conn is None:
            conn = await self.database.connection()
            release = True
        else:
            release = False

        try:
            return await conn.execute(query, *args)
        finally:
            if release:
                await conn.release()

    def transaction(self) -> TransactionContext:
        return TransactionContext(self)
