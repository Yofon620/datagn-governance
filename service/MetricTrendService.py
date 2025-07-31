# MetricTrendService.py
import time
import asyncio
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from utils.common import pct_int
from dao.metric_repo import MetricRepo

class MetricTrendService:
    def __init__(self, repo: MetricRepo):
        self.repo = repo

    @staticmethod
    def pct_int(series: pd.Series) -> int:
        return int(pd.to_numeric(series.astype(str).str.rstrip('%'), errors='coerce').mean())

    AGG_DICT = {
        'stability_scan': ('scan_failure_rate', lambda s: f"{100 - MetricTrendService.pct_int(s)}%"),  # 扫描稳定性
        'scan_timeliness': ('scan_timeliness_rate', lambda s: f"{MetricTrendService.pct_int(s)}%"),  # 扫描及时性
        'stability_clean': ('cleaning_failure_rate', lambda s: f"{100 - MetricTrendService.pct_int(s)}%"),  # 清洗稳定性
        'cleaning_timeliness': ('cleaning_timeliness_rate', lambda s: f"{MetricTrendService.pct_int(s)}%"),  # 清洗及时性
        'stability_convert': ('conversion_failure_rate', lambda s: f"{100 - MetricTrendService.pct_int(s)}%"),  # 转换稳定性
        'conversion_timeliness': ('conversion_timeliness_rate', lambda s: f"{MetricTrendService.pct_int(s)}%"),  # 转换及时性
        'stability_warehouse': ('warehousing_failure_rate', lambda s: f"{100 - MetricTrendService.pct_int(s)}%"),  # 入库稳定性
        'warehousing_timeliness': ('warehousing_timeliness_rate', lambda s: f"{MetricTrendService.pct_int(s)}%"),  # 入库及时性
        'stability_check': ('inspection_failure_rate', lambda s: f"{100 - MetricTrendService.pct_int(s)}%"),  # 校验稳定性
        'inspection_timeliness': ('inspection_timeliness_rate', lambda s: f"{MetricTrendService.pct_int(s)}%"),  # 校验及时性
        'accuracy_sample_field': ('sampling_field_accuracy', lambda s: f"{MetricTrendService.pct_int(s)}%"),  # 抽样字段准确性
        'consistency_file_record': ('record_count_consistency_rate', lambda s: f"{MetricTrendService.pct_int(s)}%"),  # 文件记录数一致性
        'completeness_file_field': ('file_field_completeness_rate', lambda s: f"{MetricTrendService.pct_int(s)}%"),  # 文件字段完整性
        'uniqueness_primary_key': ('primary_key_uniqueness_rate', lambda s: f"{MetricTrendService.pct_int(s)}%"),  # 主键唯一性
        'normativity_field_format': ('field_format_normativity_rate', lambda s: f"{MetricTrendService.pct_int(s)}%")  # 字段格式规范率
    }


    async def build_metric(self, date_str: str) -> pd.DataFrame:
        # 一次性读库
        df_raw = await self.repo.load_data(date_str)
        df = df_raw.astype({
            'interface_id': str, 'department': str, 'create_time': str,
            'metric_type': str, 'biz_name': str, 'level': str,
            'interface_id_op': str, 'pt': str
        })
        df['data_date'] = pd.to_datetime(df['data_date'], errors='coerce')
        df['create_time'] = pd.to_datetime(df['create_time'], errors='coerce')
        dt_point = datetime.strptime(date_str, '%Y%m%d')

        # 并发计算
        day_df, week_df, month_df = await asyncio.gather(
            self._day(df, dt_point),
            self._week(df, dt_point),
            self._month(df, dt_point)
        )

        # 合并
        result = pd.concat([day_df, week_df, month_df], ignore_index=True)
        base_ms = int(time.time() * 1000)
        result['metric_trend_id'] = (base_ms + np.arange(len(result))).astype(str)
        result['metric_type'] = '-'
        # 去重后输出
        result = result.drop_duplicates(
            subset=['interface_id', 'department', 'biz_name', 'level', 'statistic_cycle', 'create_time', 'statistic_week_month'])
        result.to_csv('data_fabric_metric_trend.csv', index=False, encoding='utf_8_sig')
        print(f"并发完成，共 {len(result)} 条")
        return result

    # —————————— 并发子任务 ——————————
    async def _day(self, df: pd.DataFrame, dt: datetime) -> pd.DataFrame:
        return (
            df[df['create_time'] == dt]
            .groupby(['interface_id', 'department', 'create_time', 'biz_name', 'level'], as_index=False)
            .agg(**self.AGG_DICT)
            .assign(statistic_cycle=1,
                    create_time=dt.strftime('%Y%m%d'),
                    statistic_week_month=dt.strftime('%Y%m%d'))
        )

    async def _week(self, df: pd.DataFrame, dt: datetime) -> pd.DataFrame:
        week_windows = [(dt - timedelta(days=i*7+6), dt - timedelta(days=i*7)) for i in range(4)]
        dfs = []
        for w_start, w_end in week_windows:
            df_week = (
                df[(df['data_date'] >= w_start) & (df['data_date'] <= w_end)]
                .groupby(['interface_id', 'department', 'create_time', 'biz_name', 'level'], as_index=False)
                .agg(**self.AGG_DICT)
                .assign(statistic_cycle=2,
                        create_time=dt.strftime('%Y%m%d'),
                        statistic_week_month=f"{w_start.strftime('%Y%m%d')}-{w_end.strftime('%Y%m%d')}")
            )
            dfs.append(df_week)
        return pd.concat(dfs, ignore_index=True)

    async def _month(self, df: pd.DataFrame, dt: datetime) -> pd.DataFrame:
        first_day = (dt.replace(day=1) - pd.offsets.MonthBegin(1))
        last_day = first_day + pd.offsets.MonthEnd(1)
        return (
            df[(df['data_date'] >= first_day) & (df['data_date'] <= last_day)]
            .groupby(['interface_id', 'department', 'create_time', 'biz_name', 'level'], as_index=False)
            .agg(**self.AGG_DICT)
            .assign(statistic_cycle=3,
                    create_time=dt.strftime('%Y%m%d'),
                    statistic_week_month=first_day.strftime('%Y%m'))
        )