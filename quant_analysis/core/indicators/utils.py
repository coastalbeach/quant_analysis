#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
指标计算工具模块

功能：
1. 提供数据对齐工具，确保不同指标的计算结果可以正确对齐
2. 提供异常处理机制，处理缺失值和异常值
3. 提供数据转换工具，支持不同格式数据的转换
4. 提供指标结果的可视化辅助函数
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Union, Tuple, Optional, Any

# 初始化日志
logger = logging.getLogger('quant.indicators.utils')


def align_dataframes(dfs: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    对齐多个DataFrame的索引
    
    Args:
        dfs: DataFrame列表
        
    Returns:
        List[pd.DataFrame]: 对齐后的DataFrame列表
    """
    if not dfs or len(dfs) < 2:
        return dfs
    
    # 获取所有DataFrame的索引的交集
    common_index = dfs[0].index
    for df in dfs[1:]:
        common_index = common_index.intersection(df.index)
    
    # 使用交集重新索引每个DataFrame
    aligned_dfs = [df.reindex(common_index) for df in dfs]
    
    return aligned_dfs


def handle_missing_values(df: pd.DataFrame, method: str = 'ffill', limit: int = None) -> pd.DataFrame:
    """
    处理DataFrame中的缺失值
    
    Args:
        df: 输入DataFrame
        method: 填充方法，可选值：'ffill'(前向填充)、'bfill'(后向填充)、'interpolate'(插值)
        limit: 连续填充的最大数量
        
    Returns:
        pd.DataFrame: 处理后的DataFrame
    """
    if df.empty:
        return df
    
    result = df.copy()
    
    if method == 'ffill':
        result = result.fillna(method='ffill', limit=limit)
    elif method == 'bfill':
        result = result.fillna(method='bfill', limit=limit)
    elif method == 'interpolate':
        result = result.interpolate(method='linear', limit=limit, limit_direction='both')
    
    return result


def handle_outliers(series: pd.Series, method: str = 'clip', std_dev: float = 3.0) -> pd.Series:
    """
    处理Series中的异常值
    
    Args:
        series: 输入Series
        method: 处理方法，可选值：'clip'(截断)、'remove'(删除)
        std_dev: 标准差倍数，用于确定异常值的阈值
        
    Returns:
        pd.Series: 处理后的Series
    """
    if series.empty:
        return series
    
    # 计算均值和标准差
    mean = series.mean()
    std = series.std()
    
    # 设置上下限
    lower_bound = mean - std_dev * std
    upper_bound = mean + std_dev * std
    
    if method == 'clip':
        # 截断异常值
        return series.clip(lower=lower_bound, upper=upper_bound)
    elif method == 'remove':
        # 将异常值设为NaN
        mask = (series < lower_bound) | (series > upper_bound)
        result = series.copy()
        result[mask] = np.nan
        return result
    else:
        return series


def normalize_data(data: Union[pd.Series, pd.DataFrame], method: str = 'minmax') -> Union[pd.Series, pd.DataFrame]:
    """
    归一化数据
    
    Args:
        data: 输入数据，可以是Series或DataFrame
        method: 归一化方法，可选值：'minmax'(最小-最大归一化)、'zscore'(Z-score标准化)
        
    Returns:
        Union[pd.Series, pd.DataFrame]: 归一化后的数据
    """
    if isinstance(data, pd.Series):
        if method == 'minmax':
            min_val = data.min()
            max_val = data.max()
            if max_val == min_val:
                return pd.Series(0.5, index=data.index)
            return (data - min_val) / (max_val - min_val)
        elif method == 'zscore':
            mean = data.mean()
            std = data.std()
            if std == 0:
                return pd.Series(0, index=data.index)
            return (data - mean) / std
    elif isinstance(data, pd.DataFrame):
        result = pd.DataFrame(index=data.index)
        for col in data.columns:
            result[col] = normalize_data(data[col], method)
        return result
    
    return data


def combine_indicators(indicators: List[pd.DataFrame], weights: List[float] = None) -> pd.Series:
    """
    组合多个指标为一个综合指标
    
    Args:
        indicators: 指标DataFrame列表
        weights: 权重列表，默认为等权重
        
    Returns:
        pd.Series: 组合后的指标
    """
    if not indicators:
        return pd.Series()
    
    # 对齐所有指标
    aligned_indicators = align_dataframes(indicators)
    
    # 如果没有提供权重，使用等权重
    if weights is None:
        weights = [1.0 / len(aligned_indicators)] * len(aligned_indicators)
    
    # 确保权重数量与指标数量一致
    if len(weights) != len(aligned_indicators):
        logger.warning(f"权重数量({len(weights)})与指标数量({len(aligned_indicators)})不一致，使用等权重")
        weights = [1.0 / len(aligned_indicators)] * len(aligned_indicators)
    
    # 归一化每个指标
    normalized_indicators = [normalize_data(ind) for ind in aligned_indicators]
    
    # 加权组合
    result = pd.Series(0, index=normalized_indicators[0].index)
    for i, indicator in enumerate(normalized_indicators):
        # 如果指标是DataFrame，取第一列
        if isinstance(indicator, pd.DataFrame):
            indicator = indicator.iloc[:, 0]
        result += weights[i] * indicator
    
    return result


def resample_data(data: pd.DataFrame, freq: str) -> pd.DataFrame:
    """
    重采样数据到指定频率
    
    Args:
        data: 输入数据，必须有日期索引
        freq: 目标频率，例如'W'(周)、'M'(月)、'Q'(季)、'Y'(年)
        
    Returns:
        pd.DataFrame: 重采样后的数据
    """
    if data.empty or not isinstance(data.index, pd.DatetimeIndex):
        logger.warning("数据为空或索引不是日期类型，无法进行重采样")
        return data
    
    # 确保数据按日期排序
    data = data.sort_index()
    
    # 定义OHLCV列的重采样规则
    ohlc_dict = {
        '开盘': 'first',
        '最高': 'max',
        '最低': 'min',
        '收盘': 'last',
        '成交量': 'sum',
        '成交额': 'sum'
    }
    
    # 创建重采样规则字典
    agg_dict = {}
    for col in data.columns:
        if col in ohlc_dict:
            agg_dict[col] = ohlc_dict[col]
        else:
            # 对于其他列，使用最后一个值
            agg_dict[col] = 'last'
    
    # 执行重采样
    resampled = data.resample(freq).agg(agg_dict)
    
    return resampled