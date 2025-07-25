import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from utils.common import pct_int, pct_ratio
from dao.quality_repo import QualityRepo

class QualityService:
    def __init__(self, repo: QualityRepo):
        self.repo = repo

    async def build_quality(self, date_str: str) -> pd.DataFrame:
        today = datetime.strptime(date_str, "%Y%m%d")
        yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")

        df = await self.repo.load_business_level(yesterday, date_str)
        if df.empty:
            return pd.DataFrame()

        print(f"读取业务级别表{len(df)}个")

        # 统一类型
        df['stability'] = df['stability'].astype(str).str.rstrip('%').astype(float)
        df['timeliness'] = df['timeliness'].astype(str).str.rstrip('%').astype(float)

        # 先按 department、statistic_cycle、create_time 聚合当天值
        today_df = (
            df[df['create_time'] == date_str]
            .groupby(['department', 'statistic_cycle', 'create_time'], as_index=False)
            .agg(
                stability=('stability', lambda x: f"{pct_int(x)}%"),
                timeliness=('timeliness', lambda x: f"{pct_int(x)}%")
            )
        )
        print(f"当天聚合计算结果{len(today_df)}个")
        # 计算环比（与昨天同维度）
        yest_df = (
            df[df['create_time'] == yesterday]
            .groupby(['department', 'statistic_cycle'], as_index=False)
            .agg(
                yest_stability=('stability', 'mean'),
                yest_timeliness=('timeliness', 'mean')
            )
        )
        print(f"昨天聚合计算结果{len(today_df)}个")
        # 合并计算环比
        result = (
            today_df
            .merge(yest_df, on=['department', 'statistic_cycle'], how='left')
            .assign(
                stability_ratio=lambda r: pct_ratio(r['stability'], r['yest_stability']),
                timeliness_ratio=lambda r: pct_ratio(r['timeliness'], r['yest_timeliness']),
                object_type='1',
                create_time=date_str
            )
            .drop(columns=['yest_stability', 'yest_timeliness'])
        )


        base_ms = int(time.time() * 1000)
        result['interface_quality_id'] = (base_ms + np.arange(len(result))).astype(str)

        result.to_csv('data_fabric_interface_quality.csv', index=False, encoding='utf_8_sig')
        return result