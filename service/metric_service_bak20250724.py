import time
import numpy as np
import pandas as pd
from datetime import datetime

from dao.metric_repo import MetricRepo

class MetricService:
    def __init__(self, repo: MetricRepo):
        self.repo = repo

    async def build_metric(self, date_str: str) -> pd.DataFrame:
        df = await self.repo.load_data(date_str)

        # 强制字符串，避免 groupby 报错
        df = df.astype({
            'interface_id': str,
            'department': str,
            'create_time': str,
            'statistic_cycle': str,
            'metric_type': str,
            'biz_name': str,
            'level': str,
            'interface_id_op': str,
            'pt': str
        })
        print(f"已完成读取接口驾驶舱明细表数据{len(df)}个")
        def pct_int(series):
            return series.str.rstrip('%').astype(float).mean().astype(int)

        group_result = (
            df.groupby(
                ['interface_id', 'department', 'create_time', 'biz_name', 'level'],
                as_index=False
            )
            .agg(
                stability_scan=('scan_failure_rate', lambda x: f"{100 - pct_int(x)}%"),  # 扫描稳定性
                scan_timeliness=('scan_timeliness_rate', lambda x: f"{pct_int(x)}%"),  # 扫描及时性
                stability_clean=('cleaning_failure_rate', lambda x: f"{100 - pct_int(x)}%"),  # 清洗稳定性
                cleaning_timeliness=('cleaning_timeliness_rate', lambda x: f"{pct_int(x)}%"),  # 清洗及时性
                stability_convert=('conversion_failure_rate', lambda x: f"{100 - pct_int(x)}%"),  # 转换稳定性
                conversion_timeliness=('conversion_timeliness_rate', lambda x: f"{pct_int(x)}%"),  # 转换及时性
                stability_warehouse=('warehousing_failure_rate', lambda x: f"{100 - pct_int(x)}%"),  # 入库稳定性
                warehousing_timeliness=('warehousing_timeliness_rate', lambda x: f"{pct_int(x)}%"),  # 入库及时性
                stability_check=('inspection_failure_rate', lambda x: f"{100 - pct_int(x)}%"),  # 校验稳定性
                inspection_timeliness=('inspection_timeliness_rate', lambda x: f"{pct_int(x)}%"),  # 校验及时性
                accuracy_sample_field=('sampling_field_accuracy', lambda x: f"{pct_int(x)}%"),  # 抽样字段准确性
                consistency_file_record=('record_count_consistency_rate', lambda x: f"{pct_int(x)}%"),  # 文件记录数一致性
                completeness_file_field=('file_field_completeness_rate', lambda x: f"{pct_int(x)}%"),  # 文件字段完整性
                uniqueness_primary_key=('primary_key_uniqueness_rate', lambda x: f"{pct_int(x)}%"),  # 主键唯一性
                normativity_field_format=('field_format_normativity_rate', lambda x: f"{pct_int(x)}%")  # 字段格式规范率
            )
        )

        # 用毫秒级时间戳作为 metric_trend_id
        base_ms = int(time.time() * 1000)
        final_tmp = group_result.assign(
            metric_trend_id=lambda x: (base_ms + np.arange(len(x))).astype(str),  # 主键ID
            statistic_week_month='-',  # 统计周或月份
        )
        # 避免主键重复
        final = final_tmp.drop_duplicates(subset=['interface_id', 'create_time'])
        print(f"已完成数据处理{len(final)}个")
        filename = f"data_fabric_metric_trend.csv"
        final.to_csv(filename, index=False, encoding='utf_8_sig')
        return final