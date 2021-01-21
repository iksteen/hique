from __future__ import annotations

from typing import Any, Dict, List

import asyncpg.transaction

from hique.database import Connection, Database, Transaction
from hique.pgbuilder import PostgresqlQueryBuilder


class PostgresqlDatabasePool(Database):
    query_builder = PostgresqlQueryBuilder()
    pool: asyncpg.pool.Pool

    def __init__(self) -> None:
        pass

    async def init(self, *args: Any, **kwargs: Any) -> None:
        self.pool = await asyncpg.create_pool(*args, **kwargs)

    async def connection(self) -> PostgresqlConnection:
        conn = await self.pool.acquire()
        return PostgresqlConnection(self.pool, conn)


class PostgresqlConnection(Connection):
    def __init__(self, pool: asyncpg.pool.Pool, conn: asyncpg.Connection):
        self.pool = pool
        self.conn = conn

    async def execute(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        return list(map(dict, await self.conn.fetch(query, *args)))

    async def transaction(self) -> PostgresqlTransaction:
        transaction = self.conn.transaction()
        await transaction.start()
        return PostgresqlTransaction(transaction)

    async def release(self) -> None:
        await self.pool.release(self.conn)


class PostgresqlTransaction(Transaction):
    def __init__(self, transaction: asyncpg.transaction.Transaction):
        self.transaction = transaction

    async def commit(self) -> None:
        await self.transaction.commit()

    async def rollback(self) -> None:
        await self.transaction.rollback()
