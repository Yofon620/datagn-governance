import time
import numpy as np
import pandas as pd
from dao.business_repo import BusinessLevelRepo

class BusinessLevelService:
    def __init__(self, repo: BusinessLevelRepo):
        self.repo = repo

    def pct_mean(self, s: pd.Series) -> str:
        """去掉 % 取平均 → 整数 → 补 %"""
        return f"{s.str.rstrip('%').astype(float).mean().astype(int)}%"

    async def build_aggregate(self, date_str: str) -> pd.DataFrame:
        """在 data_fabric_metric_trend 上按业务维度聚合"""
        df = await self.repo.load_data(date_str)

        # 统一转成字符串，防止 groupby 报错
        df = df.astype({
            'interface_id': str,
            'department': str,
            'create_time': str,
            'statistic_cycle': str,
            'metric_type': str,
            'biz_name': str,
            'level': str
        })
        print(f"读取趋势表数据已完成 {len(df)}")

        group_cols = ['interface_id', 'department', 'create_time', 'statistic_cycle',
                      'metric_type', 'biz_name', 'level']

        # 稳定性
        stability_cols = ['stability_scan',
                          'stability_clean',
                          'stability_convert',
                          'stability_warehouse',
                          'stability_check']
        # 及时性
        timeliness_cols = ['scan_timeliness',
                           'cleaning_timeliness',
                           'conversion_timeliness',
                           'warehousing_timeliness',
                           'inspection_timeliness']

        # 关键：用 apply 一次性计算三列
        def _calc(group: pd.DataFrame) -> pd.Series:
            return pd.Series({
                'stability': self.pct_mean(group[stability_cols].stack()),
                'timeliness': self.pct_mean(group[timeliness_cols].stack()),
                'completeness': self.pct_mean(group['completeness_file_field']),  # 完整性
                'accuracy': self.pct_mean(group['accuracy_sample_field']),  # 准确性
                'consistency': self.pct_mean(group['consistency_file_record']),  # 一致性
                'uniqueness': self.pct_mean(group['uniqueness_primary_key']),  # 唯一性
                'normativity': self.pct_mean(group['normativity_field_format'])  # 规范性
            })

        group_result_tmp = (
            df
            .groupby(group_cols, group_keys=False)
            .apply(_calc, include_groups=False)
            .reset_index()
        )

        # 重命名
        final_tmp = group_result_tmp.rename(columns={
            'metric_type': 'object_type'
        })

        base_ms = int(time.time() * 1000)
        final = final_tmp.assign(
            interface_quality_scale_id=lambda x: (base_ms + np.arange(len(x))).astype(str)  # 主键ID
        )
        final = final.drop_duplicates(subset=['interface_id', 'create_time'])
        print(f"业务级数据处理已完成 {len(final)}")
        filename = "data_fabric_interface_business_level.csv"
        final.to_csv(filename, index=False, encoding='utf_8_sig')
        return final