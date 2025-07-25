# service/quality_scale_service.py
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dao.quality_scale_repo import QualityScaleRepo

class QualityScaleService:
    def __init__(self, repo: QualityScaleRepo):
        self.repo = repo

    def _to_float(self, s: pd.Series) -> pd.Series:
        """去掉 % 并转 float"""
        return s.astype(str).str.rstrip('%').astype(float)

    def calc_ratio(self, today_val: float, yest_val: float) -> float:
        """环比公式：(today - yest) / yest"""
        """环比百分比：乘100→取整→补%"""
        if yest_val == 0 or pd.isna(yest_val):
            return "0%"
        ratio = int(round((today_val - yest_val) / yest_val * 100))
        return f"{ratio}%"

    async def build_ratio(self, today_str: str) -> pd.DataFrame:
        """
        计算 stability_ratio、timeliness_ratio
        """
        today = datetime.strptime(today_str, "%Y%m%d")
        yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")

        df = await self.repo.load_business_level(yesterday, today_str)

        # ---------- 关键：先把字符串列转数值 ----------
        df["stability"] = self._to_float(df["stability"])
        df["timeliness"] = self._to_float(df["timeliness"])
        # 把日期列统一成字符串以便 pivot
        df["create_time"] = df["create_time"].dt.strftime("%Y%m%d")
        # ---------------------------------------------
        pivot = df.pivot_table(
            index=["interface_id", "department", "statistic_cycle", "object_type"],
            columns="create_time",
            values=["stability", "timeliness"],
            fill_value=None
        )

        # 扁平化列名，并强制补齐缺失列
        pivot.columns = ["_".join(col) for col in pivot.columns]
        for col in ["stability", "timeliness"]:
            for dt in [yesterday, today_str]:
                col_name = f"{col}_{dt}"
                if col_name not in pivot.columns:
                    pivot[col_name] = pd.NA  # 补空值

        pivot = pivot.reset_index()

        pivot["stability_ratio"] = pivot.apply(
            lambda r: self.calc_ratio(r["stability_" + today_str],
                                      r["stability_" + yesterday]), axis=1)
        pivot["timeliness_ratio"] = pivot.apply(
            lambda r: self.calc_ratio(r["timeliness_" + today_str],
                                      r["timeliness_" + yesterday]), axis=1)

        result = pivot[
            ["interface_id", "department", "statistic_cycle", "object_type",
             f"stability_{today_str}", f"timeliness_{today_str}",
             "stability_ratio", "timeliness_ratio"]
        ].rename(columns={
            f"stability_{today_str}": "stability",
            f"timeliness_{today_str}": "timeliness"
        })
        # 把%加回去
        result["stability"] = result["stability"].apply(lambda x: f"{int(round(x))}%" if pd.notnull(x) else "")
        result["timeliness"] = result["timeliness"].apply(lambda x: f"{int(round(x))}%" if pd.notnull(x) else "")
        result["create_time"] = today_str

        base_ms = int(time.time() * 1000)
        final = result.assign(
            interface_quality_scale_id=lambda x: (base_ms + np.arange(len(x))).astype(str),  # 主键ID
            scale_total='-',  # 接口接入总量
            scale_total_ratio='-',  # 接口接入总量环比
            scale_new='-',  # 新增接口数量
            scale_new_ratio='-',  # 新增接口数量环比
            scale_changed='-',  # 变更接口数量
            scale_changed_ratio='-',  # 变更接口数量环比
        )

        final.to_csv("data_fabric_interface_quality_scale.csv", index=False, encoding="utf_8_sig")
        return final