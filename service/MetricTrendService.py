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

    # ---------- 主流程 ----------
    async def build_metric(self, date_str: str) -> pd.DataFrame:
        # 1) 一次性读库
        df_raw = await self.repo.load_data(date_str)
        df = df_raw.astype({
            'department': str,
            'create_time': str,
            'data_date': str,
            'scan_failure_rate': str,
            'scan_timeliness_rate': str,
            'cleaning_failure_rate': str,
            'cleaning_timeliness_rate': str,
            'conversion_failure_rate': str,
            'conversion_timeliness_rate': str,
            'warehousing_failure_rate': str,
            'warehousing_timeliness_rate': str,
            'inspection_failure_rate': str,
            'inspection_timeliness_rate': str,
            'sampling_field_accuracy': str,
            'record_count_consistency_rate': str,
            'file_field_completeness_rate': str,
            'primary_key_uniqueness_rate': str,
            'field_format_normativity_rate': str
        })

        df['data_date'] = pd.to_datetime(df['data_date'], errors='coerce')
        df['create_time'] = pd.to_datetime(df['create_time'], errors='coerce')
        dt_point = datetime.strptime(date_str, '%Y%m%d')

        # 2) 拆分 biz_name
        df['biz_name'] = df['biz_name'].fillna('').astype(str)
        df = df[df['biz_name'] != '']  # 空值保留原行
        # 按“、”或“,”拆分；若无分隔符则原样保留
        df = df.assign(biz_name_split=df['biz_name'].str.split(r'[,，、]')).explode('biz_name_split').fillna('')

        # 3) 并发聚合
        day_df, week_df, month_df = await asyncio.gather(
            self._day(df, dt_point),
            self._week(df, dt_point),
            self._month(df, dt_point)
        )

        # 4) 合并并去重
        result = pd.concat([day_df, week_df, month_df], ignore_index=True)
        base_ms = int(time.time() * 1000)
        result['metric_trend_id'] = (base_ms + np.arange(len(result))).astype(str)
        # result['metric_type'] = '-'

        result.to_csv('data_fabric_metric_trend_business.csv', index=False, encoding='utf_8_sig')
        await self.repo.write_metric(result)
        print(f"趋势表完成 {len(result)} 条")
        return result

    # ------------ 子任务：直接返回业务级聚合 ------------
    async def _day(self, df: pd.DataFrame, dt: datetime) -> pd.DataFrame:

        dey_df = (
                df[df['create_time'] == dt]
                .groupby([
                    'department', 'create_time', 'biz_name_split', 'level'
                ], as_index=False)
                .agg(**self.AGG_DICT)
                .assign(statistic_cycle=1,
                        create_time=dt.strftime('%Y%m%d'),
                        statistic_week_month=dt.strftime('%Y%m%d'))
                .rename(columns={'biz_name_split': 'biz_name'})
            )
        dey_df = dey_df.drop_duplicates(subset=['department', 'biz_name', 'create_time', 'statistic_cycle', 'statistic_week_month'])
        print(f"子任务-日数据聚合已完成：{len(dey_df)}")
        # dey_df.to_csv('data_fabric_metric_trend_business_day.csv', index=False, encoding='utf_8_sig')
        return dey_df


    async def _week(self, df: pd.DataFrame, dt: datetime) -> pd.DataFrame:
        windows = [(dt - timedelta(days=i * 7 + 6), dt - timedelta(days=i * 7)) for i in range(4)]
        dfs = []
        for w_start, w_end in windows:
            df_week = (
                df[(df['data_date'] >= w_start) & (df['data_date'] <= w_end)]
                .groupby([
                    'department', 'create_time', 'biz_name_split', 'level'
                ], as_index=False)
                .agg(**self.AGG_DICT)
                .assign(statistic_cycle=2,
                        create_time=dt.strftime('%Y%m%d'),
                        statistic_week_month=f"{w_start.strftime('%Y%m%d')}-{w_end.strftime('%Y%m%d')}")
                .rename(columns={'biz_name_split': 'biz_name'})
            )
            dfs.append(df_week)

        print(f"子任务-周数据聚合已完成：{len(dfs)}")
        week_df = pd.concat(dfs, ignore_index=True)
        week_df = week_df.drop_duplicates(subset=['department', 'biz_name', 'create_time', 'statistic_cycle', 'statistic_week_month'])
        # week_df.to_csv('data_fabric_metric_trend_business_week.csv', index=False, encoding='utf_8_sig')
        return week_df



    async def _month(self, df: pd.DataFrame, dt: datetime) -> pd.DataFrame:
        first_day = (dt.replace(day=1) - pd.offsets.MonthBegin(1))
        last_day = first_day + pd.offsets.MonthEnd(1)
        month_df = (
            df[(df['data_date'] >= first_day) & (df['data_date'] <= last_day)]
            .groupby([
                'department', 'create_time', 'biz_name_split', 'level'
            ], as_index=False)
            .agg(**self.AGG_DICT)
            .assign(statistic_cycle=3,
                    create_time=dt.strftime('%Y%m%d'),
                    statistic_week_month=first_day.strftime('%Y%m'))
            .rename(columns={'biz_name_split': 'biz_name'})
        )
        print(f"子任务-月数据聚合已完成：{len(month_df)}")
        month_df = month_df.drop_duplicates(subset=['department', 'biz_name', 'create_time', 'statistic_cycle', 'statistic_week_month'])
        # month_df.to_csv('data_fabric_metric_trend_business_month.csv', index=False, encoding='utf_8_sig')
        return month_df