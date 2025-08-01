import time
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import datetime, timedelta

from dao.metric_repo import MetricRepo

class MetricService:
    def __init__(self, repo: MetricRepo):
        self.repo = repo

    # -------------- 统一工具 --------------
    @staticmethod
    def pct_int(series: pd.Series) -> int:
        return int(series.str.rstrip('%').astype(float).mean())

    # -------------- 聚合字典 --------------
    AGG_DICT = {
        'stability_scan': ('scan_failure_rate', lambda s: f"{100 - MetricService.pct_int(s)}%"),  # 扫描稳定性
        'scan_timeliness': ('scan_timeliness_rate', lambda s: f"{MetricService.pct_int(s)}%"),  # 扫描及时性
        'stability_clean': ('cleaning_failure_rate', lambda s: f"{100 - MetricService.pct_int(s)}%"),  # 清洗稳定性
        'cleaning_timeliness': ('cleaning_timeliness_rate', lambda s: f"{MetricService.pct_int(s)}%"),  # 清洗及时性
        'stability_convert': ('conversion_failure_rate', lambda s: f"{100 - MetricService.pct_int(s)}%"),  # 转换稳定性
        'conversion_timeliness': ('conversion_timeliness_rate', lambda s: f"{MetricService.pct_int(s)}%"),  # 转换及时性
        'stability_warehouse': ('warehousing_failure_rate', lambda s: f"{100 - MetricService.pct_int(s)}%"),  # 入库稳定性
        'warehousing_timeliness': ('warehousing_timeliness_rate', lambda s: f"{MetricService.pct_int(s)}%"),  # 入库及时性
        'stability_check': ('inspection_failure_rate', lambda s: f"{100 - MetricService.pct_int(s)}%"),  # 校验稳定性
        'inspection_timeliness': ('inspection_timeliness_rate', lambda s: f"{MetricService.pct_int(s)}%"),  # 校验及时性
        'accuracy_sample_field': ('sampling_field_accuracy', lambda s: f"{MetricService.pct_int(s)}%"),  # 抽样字段准确性
        'consistency_file_record': ('record_count_consistency_rate', lambda s: f"{MetricService.pct_int(s)}%"),  # 文件记录数一致性
        'completeness_file_field': ('file_field_completeness_rate', lambda s: f"{MetricService.pct_int(s)}%"),  # 文件字段完整性
        'uniqueness_primary_key': ('primary_key_uniqueness_rate', lambda s: f"{MetricService.pct_int(s)}%"),  # 主键唯一性
        'normativity_field_format': ('field_format_normativity_rate', lambda s: f"{MetricService.pct_int(s)}%"),  # 字段格式规范率
    }

    # ---------------- 主流程 ----------------
    async def build_metric(self, date_str: str) -> pd.DataFrame:
        """
        date_str: 任意一天(YYYYMMDD)，用于确定周/月范围
        """

        df_raw = await self.repo.load_data(date_str)
        # 统一字段类型
        df = df_raw.astype({
            'interface_id': str,
            'department': str,
            'create_time': str,
            'metric_type': str,
            'biz_name': str,
            'level': str,
            'interface_id_op': str,
            'pt': str
        })
        # 统一日期列
        df['data_date'] = pd.to_datetime(df['data_date'], errors='coerce')
        df['create_time'] = pd.to_datetime(df['create_time'], errors='coerce')
        print(f"已完成读取接口驾驶舱明细表数据{len(df)}个")

        dt_point = datetime.strptime(date_str, '%Y%m%d')

        # 1) 日：create_time 当天
        day_df = (
            df[df['create_time'] == dt_point]
            .groupby(['interface_id', 'department', 'create_time', 'biz_name', 'level'], as_index=False)
            .agg(**self.AGG_DICT)
            .assign(statistic_cycle=1,
                    create_time=dt_point.strftime('%Y%m%d'),
                    statistic_week_month=dt_point.strftime('%Y%m%d'))
        )

        # 周->4 个连续 7 天窗口
        week_windows = [
            (dt_point - timedelta(days=i*7+6), dt_point - timedelta(days=i*7))
            for i in range(4)          # 0,1,2,3 -> 4 个窗口
        ]

        week_dfs = []
        for idx, (w_start, w_end) in enumerate(week_windows, start=1):
            df_week = (
                df[(df['data_date'] >= w_start) & (df['data_date'] <= w_end)]
                    .groupby(['interface_id', 'department', 'create_time', 'biz_name', 'level'], as_index=False)
                    .agg(**self.AGG_DICT)
                    .assign(
                    statistic_cycle=2,
                    create_time=dt_point.strftime('%Y%m%d'),
                    statistic_week_month=f"{w_start.strftime('%Y%m%d')}-{w_end.strftime('%Y%m%d')}"
                )
            )
            week_dfs.append(df_week)

        # 把 4 个周结果合并
        week_df = pd.concat(week_dfs, ignore_index=True)


        # 月：取 **上一个月整月** 数据
        # 先算出上一个自然月 1 号与月末
        first_day = (dt_point.replace(day=1) - pd.offsets.MonthBegin(1))
        last_day = first_day + pd.offsets.MonthEnd(1)

        month_df = (
            df[(df['data_date'] >= first_day) & (df['data_date'] <= last_day)]
            .groupby(['interface_id', 'department', 'create_time', 'biz_name', 'level'], as_index=False)
            .agg(**self.AGG_DICT)
            .assign(statistic_cycle=3,
                    create_time=dt_point.strftime('%Y%m%d'),
                    statistic_week_month=first_day.strftime('%Y%m'))
        )

        # 拼接 & 主键
        result = pd.concat([day_df, week_df, month_df], ignore_index=True)
        base_ms = int(time.time() * 1000)
        result['metric_trend_id'] = (base_ms + np.arange(len(result))).astype(str)

        # 去重后输出
        result = result.drop_duplicates(
            subset=['interface_id', 'department', 'biz_name', 'level', 'statistic_cycle', 'create_time'])

        result['metric_type'] = '-'
        result.to_csv('data_fabric_metric_trend.csv', index=False, encoding='utf_8_sig')
        print(f"已完成日/周/月滚动聚合，共 {len(result)} 条")
        return result



