import time
import numpy as np
import pandas as pd

from dao.scale_repo import ScaleRepo   # 新建 DAO

class ScaleService:
    def __init__(self, repo: ScaleRepo):
        self.repo = repo

    async def build_scale(self, date_str: str) -> pd.DataFrame:
        df = await self.repo.load_quality(date_str)
        # if df.empty:
        #     return pd.DataFrame()
        #
        # # 仅按 department、statistic_cycle、create_time 分组即可
        # base = (
        #     df
        #     .groupby(['department', 'statistic_cycle', 'create_time'], as_index=False)
        #     .first()  # 任意取一行占位
        # )

        result = df.assign(
            object_type='2',
            scale_total='0',
            scale_total_ratio='0%',
            scale_new='0',
            scale_new_ratio='0%',
            scale_changed='0%',
            scale_changed_ratio='0%'
        )

        base_ms = int(time.time() * 1000)
        result['interface_scale_id'] = (base_ms + np.arange(len(result))).astype(str)

        result.to_csv('data_fabric_interface_scale.csv', index=False, encoding='utf_8_sig')
        return result