import asyncio
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

from config.settings import settings


class MysqlClient:
    def __init__(self):
        self.engine = create_async_engine(settings.url, pool_pre_ping=True)

    async def read_query(self, sql: str) -> pd.DataFrame:
        async with self.engine.connect() as conn:
            res = await conn.execute(text(sql))
            return pd.DataFrame(res.fetchall(), columns=res.keys())

    async def read_table(self, table: str) -> pd.DataFrame:
        return await self.read_query(f"SELECT * FROM {table}")

    async def write_df(self, df: pd.DataFrame, table: str, if_exists='append'):
        async with self.engine.begin() as conn:
            await conn.run_sync(
                lambda sync_conn: df.to_sql(
                    table, sync_conn, index=False, if_exists=if_exists, chunksize=10000, method='multi'
                )
            )