import time
import numpy as np
import pandas as pd

from utils.common import pct_mean
from dao.business_repo import BusinessLevelRepo
from dao.mysql_client import MysqlClient

class BusinessLevelService:
    def __init__(self, repo: BusinessLevelRepo):
        self.repo = repo
        self.dao_sql = MysqlClient()

    async def build_aggregate(self, date_str: str) -> pd.DataFrame:
        """在 data_fabric_metric_trend 上按业务维度聚合"""
        df = await self.repo.load_data(date_str)
        # 统一类型
        df = df.astype({
            'interface_id': str,
            'department': str,
            'create_time': str,
            'statistic_cycle': str,
            'biz_name': str,
            'level': str
        })
        print(f"读取趋势表数据已完成 {len(df)}")

        group_cols = [
            'interface_id', 'department', 'create_time',
            'statistic_cycle', 'biz_name', 'level'
        ]

        # 阶段列清单
        stability_cols = [
            'stability_scan', 'stability_clean', 'stability_convert',
            'stability_warehouse', 'stability_check'
        ]
        timeliness_cols = [
            'scan_timeliness', 'cleaning_timeliness', 'conversion_timeliness',
            'warehousing_timeliness', 'inspection_timeliness'
        ]

        def _calc(group: pd.DataFrame) -> pd.Series:
            return pd.Series({
                'stability':  pct_mean(group[stability_cols].stack()),
                'timeliness': pct_mean(group[timeliness_cols].stack()),
                'completeness': pct_mean(group['completeness_file_field']),
                'accuracy':  pct_mean(group['accuracy_sample_field']),
                'consistency': pct_mean(group['consistency_file_record']),
                'uniqueness': pct_mean(group['uniqueness_primary_key']),
                'normativity': pct_mean(group['normativity_field_format'])
            })

        # 1) 笛卡尔展开 object_type=[1,2]
        df = df.assign(object_type=[['1', '2']] * len(df)).explode('object_type')
        # 2) 一次聚合即可
        group_result = (
            df
            .groupby(group_cols + ['object_type'], group_keys=False)
            .apply(_calc, include_groups=False)
            .reset_index()
        )
        print(f"笛卡尔积展开结果:{len(group_result)}")
        # 3) 复制一份 object_type=2 的数据（周/月）
        type2_df = group_result.assign(object_type='2')
        # 4) 纵向合并
        final_tmp = pd.concat([group_result, type2_df], ignore_index=True)

        # 6) 去重 & 主键
        base_ms = int(time.time() * 1000)
        final = (
            final_tmp
            .assign(interface_quality_scale_id=lambda x: (base_ms + np.arange(len(x))).astype(str))
            .drop_duplicates(subset=[
                'interface_id', 'department', 'create_time',
                'statistic_cycle', 'biz_name', 'level', 'object_type'
            ])
        )
        final.to_csv(f"data_fabric_interface_business_level_tmp_{date_str}.csv", index=False, encoding='utf_8_sig')
        await self.dao_sql.write_data(final, "data_fabric_interface_business_level_tmp")
        print(f"业务级一次数据处理已完成 {len(final)},用于下一阶段质量规模表用")
        # ===== 二次聚合：按 department / create_time / biz_name =====
        final_second = (
            final
            .groupby(['department', 'create_time', 'biz_name', 'statistic_cycle', 'object_type', 'level'], as_index=False)
            .agg(
                stability=('stability', pct_mean),
                timeliness=('timeliness', pct_mean),
                completeness=('completeness', pct_mean),
                accuracy=('accuracy', pct_mean),
                consistency=('consistency', pct_mean),
                uniqueness=('uniqueness', pct_mean),
                normativity=('normativity', pct_mean)
            )
                # 二次聚合没有 interface_id/level，补空
            .assign(
                interface_business_level_id=lambda x: (
                        int(time.time() * 1000) + np.arange(len(x))
                ).astype(str)
            )
        )

        final_second.to_csv(f"data_fabric_interface_business_level_{date_str}.csv", index=False, encoding='utf_8_sig')
        print(f"最终两层聚合完成，共 {len(final_second)} 条")

        return final_second