# utils/common.py
import pandas as pd
from datetime import datetime, timedelta

def pct_int(series: pd.Series) -> int:
    """去掉%后取平均→整数；兼容非字符串"""
    s = series.astype(str).str.strip().str.rstrip('%').astype(float)
    return int(s.mean())

def pct_ratio(today: pd.Series, yest: pd.Series) -> pd.Series:
    """去掉%后转 float，再计算环比"""
    t = today.astype(str).str.rstrip('%').astype(float)
    y = yest.astype(str).str.rstrip('%').astype(float).fillna(0)
    ratio = (t - y) / y
    ratio = ratio.replace([float('inf'), float('-inf')], 0)
    return (ratio * 100).round(0).astype(int).astype(str) + '%'