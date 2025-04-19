#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
技术指标模块

包含：
1. TA-Lib封装 - 提供常用技术指标计算
2. 自定义指标 - 用户自定义的特殊指标
3. 指标计算工具 - 数据对齐和异常处理
"""

# 导出主要接口
from core.indicators.ta import (
    # 数据获取
    get_stock_data,
    get_multi_stock_data,
    
    # 指标计算
    calculate_indicators,
    
    # 移动平均线
    ma,
    ema,
    
    # 震荡指标
    macd,
    rsi,
    kdj,
    
    # 趋势指标
    adx,
    
    # 波动指标
    bollinger_bands,
    atr,
    
    # 成交量指标
    obv,
    
    # 全局实例
    ta
)