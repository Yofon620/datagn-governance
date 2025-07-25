import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from config.settings import settings, data_fabric_interface_detail_cols

class InterfaceRepo:
    def __init__(self):
        self.engine = create_async_engine(settings.url, pool_pre_ping=True)

    async def load_meta(self) -> pd.DataFrame:
        """数据治理平台-接口资源表"""
        async with self.engine.connect() as conn:
            res = await conn.execute(text("SELECT * FROM data_fabric_meta_data_interface"))
            return pd.DataFrame(res.fetchall(), columns=res.keys())

    async def load_register(self) -> pd.DataFrame:
        """数智运维平台-接口总表"""
        async with self.engine.connect() as conn:
            res = await conn.execute(text("SELECT * FROM data_interface_task_register"))
            return pd.DataFrame(res.fetchall(), columns=res.keys())

    async def load_error(self, date_str: str) -> pd.DataFrame:
        """数智运维平台-故障表"""
        sql = f"SELECT DISTINCT job_id FROM data_interface_task_error WHERE data_date='{date_str}'"
        async with self.engine.connect() as conn:
            res = await conn.execute(text(sql))
            return pd.DataFrame(res.fetchall(), columns=res.keys())

    async def load_delay(self, date_str: str) -> pd.DataFrame:
        """数智运维平台-延迟表"""
        sql = f"SELECT DISTINCT job_id FROM data_interface_task_delay WHERE data_date='{date_str}'"
        async with self.engine.connect() as conn:
            res = await conn.execute(text(sql))
            return pd.DataFrame(res.fetchall(), columns=res.keys())

    async def write_detail(self, df: pd.DataFrame):
        async with self.engine.begin() as conn:
            await conn.run_sync(
                lambda sync_conn: df[df['interface_id'].notna()]
                .to_sql("data_fabric_interface_detail",
                        sync_conn,
                        index=False,
                        if_exists="append",
                        method="multi",
                        chunksize=10000,
                        dtype=None)
            )

    async def upsert_detail(self, df: pd.DataFrame) -> tuple[int, int]:
        """
        按主键 (storage_interface_id, pt, create_time) UPSERT
        返回 (成功条数, 失败条数)
        """
        total = len(df)

        # 去重主键
        df = df.drop_duplicates(subset=['storage_interface_id', 'pt', 'create_time'])

        # 列顺序必须与表字段一致
        cols = data_fabric_interface_detail_cols
        df = df[cols]

        if df.empty:
            return 0, 0

        async with self.engine.begin() as conn:
            # 构造批量 UPSERT
            placeholders = ",".join(["%s"] * len(cols))
            updates = ",".join([f"{col}=VALUES({col})" for col in cols])

            values = [tuple(row[col] for col in cols) for row in df.itertuples(index=False)]

            sql = f"""
                INSERT INTO data_fabric_interface_detail
                ({','.join(cols)})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {updates}
            """

            await conn.execute(text(sql), *values)

    # 新增
    async def read_query(self, sql: str) -> pd.DataFrame:
        async with self.engine.connect() as conn:
            res = await conn.execute(text(sql))
            return pd.DataFrame(res.fetchall(), columns=res.keys())

