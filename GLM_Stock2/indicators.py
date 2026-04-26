"""
技术指标计算模块
提供股票常用技术指标的计算函数，基于 numpy 实现。
"""

import numpy as np


def ma(values: np.ndarray, period: int) -> np.ndarray:
    """简单移动平均线 (Simple Moving Average)。"""
    if len(values) < period:
        return np.full_like(values, np.nan, dtype=float)
    result = np.full_like(values, np.nan, dtype=float)
    cumsum = np.cumsum(np.nan_to_num(values, nan=0.0))
    result[period - 1:] = (
        cumsum[period - 1:] - np.concatenate([[0], cumsum[:-period]])
    ) / period
    nan_mask = np.isnan(values)
    result[nan_mask] = np.nan
    return result


def ema(values: np.ndarray, period: int) -> np.ndarray:
    """指数移动平均线 (Exponential Moving Average)。"""
    result = np.full_like(values, np.nan, dtype=float)
    if len(values) < period:
        return result
    valid_vals = values[~np.isnan(values)]
    if len(valid_vals) < period:
        return result
    seed = np.mean(values[:period])
    result[period - 1] = seed
    alpha = 2.0 / (period + 1)
    for i in range(period, len(values)):
        if np.isnan(values[i]):
            result[i] = result[i - 1]
        elif np.isnan(result[i - 1]):
            result[i] = values[i]
        else:
            result[i] = alpha * values[i] + (1 - alpha) * result[i - 1]
    return result


def bollinger_bands(values: np.ndarray, period: int = 20,
                    num_std: float = 2.0) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    布林带 (Bollinger Bands)。
    返回: (中轨 MA, 上轨, 下轨)
    """
    mid = ma(values, period)
    if len(values) < period:
        return mid, mid.copy(), mid.copy()
    std = np.full_like(values, np.nan, dtype=float)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        valid = window[~np.isnan(window)]
        if len(valid) > 0:
            std[i] = np.std(valid, ddof=0)
    upper = mid + num_std * std
    lower = mid - num_std * std
    return mid, upper, lower


def macd_calc(values: np.ndarray,
              fast_period: int = 12,
              slow_period: int = 26,
              signal_period: int = 9) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    MACD 指标 (Moving Average Convergence Divergence)。
    返回: (DIF / MACD线, DEA / 信号线, MACD柱状图)
    """
    ema_fast = ema(values, fast_period)
    ema_slow = ema(values, slow_period)
    dif = ema_fast - ema_slow
    dea = ema(dif, signal_period)
    histogram = 2.0 * (dif - dea)
    return dif, dea, histogram


def rsi(values: np.ndarray, period: int = 14) -> np.ndarray:
    """
    RSI 指标 (Relative Strength Index)。
    取值范围 0-100，>70 超买，<30 超卖。
    """
    result = np.full_like(values, np.nan, dtype=float)
    if len(values) < period + 1:
        return result
    deltas = np.diff(values)
    deltas = np.nan_to_num(deltas, nan=0.0)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    if avg_loss == 0:
        result[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[period] = 100.0 - 100.0 / (1.0 + rs)
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            result[i + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i + 1] = 100.0 - 100.0 / (1.0 + rs)
    return result


def max_drawdown(values: np.ndarray) -> tuple[float, int, int]:
    """
    最大回撤。
    返回: (最大回撤值, 起始索引, 结束索引)
    """
    peak_idx = 0
    max_dd = 0.0
    dd_start = 0
    dd_end = 0
    for i in range(1, len(values)):
        if values[i] > values[peak_idx]:
            peak_idx = i
        dd = (values[peak_idx] - values[i]) / values[peak_idx] if values[peak_idx] != 0 else 0
        if dd > max_dd:
            max_dd = dd
            dd_start = peak_idx
            dd_end = i
    return max_dd, dd_start, dd_end


def annualized_return(values: np.ndarray, trading_days: int = 242) -> float:
    """年化收益率。"""
    if len(values) < 2:
        return 0.0
    start_val = values[0]
    end_val = values[-1]
    if start_val <= 0 or end_val <= 0:
        return 0.0
    total_return = end_val / start_val - 1.0
    n_years = len(values) / trading_days
    if n_years <= 0:
        return 0.0
    return (1.0 + total_return) ** (1.0 / n_years) - 1.0


def annualized_volatility(values: np.ndarray, trading_days: int = 242) -> float:
    """年化波动率。"""
    if len(values) < 2:
        return 0.0
    returns = np.diff(np.log(np.maximum(values, 1e-10)))
    returns = returns[~np.isnan(returns)]
    if len(returns) == 0:
        return 0.0
    return np.std(returns, ddof=1) * np.sqrt(trading_days)


def sharpe_ratio(values: np.ndarray, risk_free_rate: float = 0.02,
                 trading_days: int = 242) -> float:
    """夏普比率 (Sharpe Ratio)。"""
    ann_ret = annualized_return(values, trading_days)
    ann_vol = annualized_volatility(values, trading_days)
    if ann_vol == 0:
        return 0.0
    return (ann_ret - risk_free_rate) / ann_vol
