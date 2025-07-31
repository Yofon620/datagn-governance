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

        # 字段列表
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
        # 3) 第一层-->result:用以计算后续质量表用
        base_ms = int(time.time() * 1000)
        level1 = (
            group_result
            .assign(interface_business_level_id=lambda x: (base_ms + np.arange(len(x))).astype(str))
            .drop_duplicates()
        )

        # level1.to_csv(f"data_fabric_interface_business_level_tmp_{date_str}.csv", index=False, encoding='utf_8_sig')
        level1.to_csv(f"data_fabric_interface_business_level_tmp.csv", index=False, encoding='utf_8_sig')
        await self.dao_sql.write_data(level1, "data_fabric_interface_business_level_tmp")
        print(f"业务级一次数据处理已完成 {len(level1)},用于下一阶段质量规模表用")

        # ===== 二次聚合：按 department / create_time / biz_name =====
        # level2：业务级（笛卡尔补全 + 聚合）
        cart = (
            df[['department', 'create_time', 'statistic_cycle', 'object_type']].drop_duplicates()
            .merge(df[['biz_name', 'level']].drop_duplicates(), how='cross')
        )

        agg2 = (
            level1.groupby(['department', 'create_time', 'statistic_cycle', 'object_type', 'biz_name', 'level'])
            .agg(
                stability=('stability', pct_mean),
                timeliness=('timeliness', pct_mean),
                completeness=('completeness', pct_mean),
                accuracy=('accuracy', pct_mean),
                consistency=('consistency', pct_mean),
                uniqueness=('uniqueness', pct_mean),
                normativity=('normativity', pct_mean)
            )
            .reset_index()
        )

        level2 = (
            cart.merge(agg2, how='left')
                .fillna('-')
                .assign(
                interface_business_level_id=lambda x: (
                        int(time.time() * 1000) + np.arange(len(x))
                ).astype(str)
            )
        )

        # level2.to_csv(f"data_fabric_interface_business_{date_str}.csv", index=False, encoding='utf_8_sig')
        level2.to_csv(f"data_fabric_interface_business.csv", index=False, encoding='utf_8_sig')
        print(f"最终两层聚合完成，共 {len(level2)} 条")

        return level2