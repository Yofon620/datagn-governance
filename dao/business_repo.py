# dao/metric_repo.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text
import pandas as pd
from config.settings import settings

class BusinessLevelRepo:
    def __init__(self):
        self.engine = create_async_engine(settings.url)

    async def load_data(self, date_str: str) -> pd.DataFrame:
        """数据治理平台-运营驾驶舱趋势表"""
        sql = f"""
            SELECT *
            FROM data_fabric_metric_trend
            WHERE create_time = '{date_str}'
        """
        async with self.engine.connect() as conn:
            res = await conn.execute(text(sql))
            return pd.DataFrame(res.fetchall(), columns=res.keys())


    async def write_data(self, df: pd.DataFrame) -> None:
        """
        将指标 DataFrame 异步写入 data_fabric_interface_business_level 表。
        """
        async with self.engine.begin() as conn:
            await conn.run_sync(
                lambda sync_conn: df.to_sql(
                    "data_fabric_interface_business_level",
                    sync_conn,
                    index=False,
                    if_exists="append",
                    method="multi",
                    chunksize=10000,
                    dtype=None
                )
            )
