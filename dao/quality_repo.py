# dao/quality_scale_repo.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text
import pandas as pd
from config.settings import settings

class QualityRepo:
    def __init__(self):
        self.engine = create_async_engine(settings.url)

    async def load_business_level(self, yesterday: str, today: str) -> pd.DataFrame:
        """
        取两天的数据，字段重命名后直接返回
        """
        sql = text("""
            SELECT *
            FROM   data_fabric_interface_business_level
            WHERE  create_time IN (:yesterday, :today)
        """)
        async with self.engine.connect() as conn:
            res = await conn.execute(sql, {"yesterday": yesterday, "today": today})
            df = pd.DataFrame(res.fetchall(), columns=res.keys())
        # 把日期列转 datetime，便于对齐
        df["create_time"] = pd.to_datetime(df["create_time"])
        return df

    async def write_quality(self, df: pd.DataFrame) -> None:
        """
        写入 data_fabric_interface_quality
        """
        async with self.engine.begin() as conn:
            await conn.run_sync(
                lambda sync_conn: df.to_sql(
                    "data_fabric_interface_quality",
                    sync_conn,
                    index=False,
                    if_exists="append",
                    method="multi",
                    chunksize=10000
                )
            )