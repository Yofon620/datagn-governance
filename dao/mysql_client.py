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


    async def write_data(self, df: pd.DataFrame, table_name: str) -> None:
        """
        将指标 DataFrame 异步写入数据表。
        """
        async with self.engine.begin() as conn:
            await conn.run_sync(
                lambda sync_conn: df.to_sql(
                    table_name,
                    sync_conn,
                    index=False,
                    if_exists="append",
                    method="multi",
                    chunksize=10000,
                    dtype=None
                )
            )
