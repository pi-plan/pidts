from typing import Any, Dict, List

from aiomysql import create_pool
from aiomysql.connection import Connection
from aiomysql.cursors import DictCursor

from common.dsn import DSN
from apply.client.client import Client


class AIOMySQL(Client):

    @classmethod
    def new(cls, dsn: DSN, minsize: int = 1, maxsize: int = 10,
            pool_recycle: int = -1) -> 'AIOMySQL':
        return cls(dsn, minsize, maxsize, pool_recycle)

    def __init__(self, dsn: DSN, minsize: int, maxsize: int,
                 pool_recycle: int):
        self.dsn = dsn
        self.minsize = minsize
        self.maxsize = maxsize
        self.pool_recycle = pool_recycle

    async def connect(self):
        self.pool = await create_pool(minsize=self.minsize,
                                      maxsize=self.maxsize,
                                      pool_recycle=self.pool_recycle,
                                      cursorclass=DictCursor,
                                      **self.dsn.get_args())

    async def execute(self, sql: str) -> int:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                return await cur.execute(sql)

    async def query(self, sql: str) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                return await cur.fetchall()

    async def acquire(self) -> Connection:
        return await self.pool.acquire()

    def release(self, conn: Connection):
        self.pool.release(conn)

    def close(self):
        self.pool.close()
