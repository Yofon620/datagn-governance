# BusinessLevelService.py
import time
import asyncio
import numpy as np
import pandas as pd
from datetime import datetime
from utils.common import pct_mean
from dao.business_repo import BusinessLevelRepo
from dao.mysql_client import MysqlClient

class BusinessLevelService:
    def __init__(self, repo: BusinessLevelRepo):
        self.repo = repo
        self.dao_sql = MysqlClient()

    # ---------- 列映射 ----------
    COL_MAP = {
        'stability_scan'      : 'stability',
        'stability_clean'     : 'stability',
        'stability_convert'   : 'stability',
        'stability_warehouse' : 'stability',
        'stability_check'     : 'stability',

        'scan_timeliness'      : 'timeliness',
        'cleaning_timeliness'  : 'timeliness',
        'conversion_timeliness': 'timeliness',
        'warehousing_timeliness': 'timeliness',
        'inspection_timeliness': 'timeliness',

        'completeness_file_field' : 'completeness',
        'accuracy_sample_field'   : 'accuracy',
        'consistency_file_record' : 'consistency',
        'uniqueness_primary_key'  : 'uniqueness',
        'normativity_field_format': 'normativity'
    }

    # 显式反向映射
    GROUP_COLS = {
        'stability':   ['stability_scan', 'stability_clean', 'stability_convert',
                        'stability_warehouse', 'stability_check'],
        'timeliness':  ['scan_timeliness', 'cleaning_timeliness', 'conversion_timeliness',
                        'warehousing_timeliness', 'inspection_timeliness'],
        'completeness': ['completeness_file_field'],
        'accuracy':    ['accuracy_sample_field'],
        'consistency': ['consistency_file_record'],
        'uniqueness':  ['uniqueness_primary_key'],
        'normativity': ['normativity_field_format']
    }

    def _calc(self, group: pd.DataFrame) -> pd.Series:
        return pd.Series({
            k: pct_mean(group[cols].stack()) for k, cols in self.GROUP_COLS.items()
        })

    # ---------- 主流程 ----------
    async def build_aggregate(self, date_str: str) -> pd.DataFrame:
        df_raw = await self.repo.load_data(date_str)
        df = df_raw.astype({
            'department': str, 'create_time': str, 'statistic_cycle': str,
            'stability_scan': str, 'stability_clean': str, 'stability_convert': str,
            'stability_warehouse': str, 'stability_check': str,
            'scan_timeliness': str, 'cleaning_timeliness': str,
            'conversion_timeliness': str, 'warehousing_timeliness': str,
            'inspection_timeliness': str, 'completeness_file_field': str,
            'accuracy_sample_field': str, 'consistency_file_record': str,
            'uniqueness_primary_key': str, 'normativity_field_format': str,
            'biz_name': str, 'level': str
        })

        # 并发聚合
        level1_obj1, level1_obj2 = await asyncio.gather(
            self._agg_obj1(df),
            self._agg_obj2(df)
        )

        final = pd.concat([level1_obj1, level1_obj2], ignore_index=True)
        final['interface_business_level_id'] = (
            int(time.time() * 1000) + np.arange(len(final))
        ).astype(str)
        final.to_csv('data_fabric_interface_business.csv', index=False, encoding='utf_8_sig')
        await self.dao_sql.write_data(final, "data_fabric_interface_business")
        print(f"业务级聚合完成 {len(final)} 条")
        return final

    # ---------- 并发聚合 ----------
    async def _agg_obj1(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .groupby(['department', 'create_time', 'statistic_cycle', 'biz_name'], as_index=False)
            .apply(self._calc, include_groups=False)
            .reset_index()
            .assign(object_type='1')
        )

    async def _agg_obj2(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .groupby(['department', 'create_time', 'statistic_cycle', 'level'], as_index=False)
            .apply(self._calc, include_groups=False)
            .reset_index()
            .assign(object_type='2')
        )