# utils/common.py
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def pct_int(series: pd.Series) -> int:
    """兼容 NaN / '-' / inf"""
    s = pd.to_numeric(series.astype(str).str.rstrip('%'), errors='coerce')
    val = s.mean()
    return int(val) if not pd.isna(val) else 0

def pct_mean(s: pd.Series) -> str:
    """去掉 % 取平均 → 整数 → 补 %"""
    return f"{s.str.rstrip('%').astype(float).mean().astype(int)}%"

def pct_ratio(today: pd.Series, yest: pd.Series) -> pd.Series:
    """去掉%后转 float，再计算环比"""
    """兼容 NaN / '-' / inf"""
    t = pd.to_numeric(today.astype(str).str.rstrip('%'), errors='coerce').fillna(0)
    y = pd.to_numeric(yest.astype(str).str.rstrip('%'), errors='coerce').fillna(0)
    ratio = (t - y) / y.replace(0, np.nan)
    return ratio.fillna(0).round(0).astype(int).astype(str) + '%'
