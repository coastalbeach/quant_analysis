#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TA-Lib技术指标封装模块

功能：
1. 从数据库'股票行情_后复权'表获取股票历史数据
2. 封装TA-Lib库提供的常用技术指标计算函数
3. 支持单只股票和批量股票的指标计算
4. 提供数据对齐和异常处理机制
5. 支持自定义参数的指标计算
"""

import logging
import pandas as pd
import numpy as np
import talib
from typing import List, Dict, Union, Tuple, Optional, Any
from psycopg.sql import SQL, Identifier
from core.storage import storage

# 初始化日志
logger = logging.getLogger('quant.indicators.ta')

# 设置常量
DEFAULT_PERIOD = "日线"  # 默认使用日线数据
DEFAULT_ADJUST = "后复权"  # 默认使用后复权数据


class TAIndicator:
    """
    TA-Lib技术指标封装类
    """
    
    def __init__(self):
        """
        初始化TA指标类
        """
        self.table_name = "股票行情_后复权"  # 默认从后复权表获取数据
    
    def get_stock_data(self, stock_code: str, start_date: str = None, end_date: str = None, 
                       period: str = DEFAULT_PERIOD, fields: List[str] = None) -> pd.DataFrame:
        """
        从数据库获取单只股票的历史数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            period: 数据频率，默认为日线
            fields: 需要获取的字段列表，默认为None（获取所有字段）
            
        Returns:
            pd.DataFrame: 股票历史数据
        """
        try:
            # 构建查询条件
            conditions = [f"\"股票代码\" = '{stock_code}'", f"\"频率\" = '{period}'"]
            if start_date:
                conditions.append(f"\"日期\" >= '{start_date}'")
            if end_date:
                conditions.append(f"\"日期\" <= '{end_date}'")
            
            # 构建查询字段
            if fields is None:
                fields_str = "*"
            else:
                fields_str = ", ".join([f'"{field}"' for field in fields])
            
            # 构建完整查询
            query = f"SELECT {fields_str} FROM \"{self.table_name}\" WHERE {' AND '.join(conditions)} ORDER BY \"日期\" ASC"
            
            # 执行查询
            conn = storage._get_connection()
            try:
                df = pd.read_sql_query(query, conn)
                if df.empty:
                    logger.warning(f"未找到股票 {stock_code} 的历史数据")
                    return pd.DataFrame()
                
                # 确保日期列为日期类型
                if '日期' in df.columns:
                    df['日期'] = pd.to_datetime(df['日期'])
                    df.set_index('日期', inplace=True)
                
                return df
            finally:
                storage._return_connection(conn)
                
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 历史数据失败: {str(e)}")
            return pd.DataFrame()
    
    def get_multi_stock_data(self, stock_codes: List[str], start_date: str = None, end_date: str = None,
                            period: str = DEFAULT_PERIOD, fields: List[str] = None) -> Dict[str, pd.DataFrame]:
        """
        批量获取多只股票的历史数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            period: 数据频率，默认为日线
            fields: 需要获取的字段列表，默认为None（获取所有字段）
            
        Returns:
            Dict[str, pd.DataFrame]: 股票代码到历史数据的映射
        """
        result = {}
        for stock_code in stock_codes:
            df = self.get_stock_data(stock_code, start_date, end_date, period, fields)
            if not df.empty:
                result[stock_code] = df
        return result
    
    def _prepare_ohlcv(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        准备OHLCV数据用于TA-Lib计算
        
        Args:
            data: 包含OHLCV数据的DataFrame
            
        Returns:
            Tuple: (open, high, low, close, volume)数组
        """
        # 检查必要的列是否存在
        required_cols = ['开盘', '最高', '最低', '收盘', '成交量']
        for col in required_cols:
            if col not in data.columns:
                raise ValueError(f"数据中缺少必要的列: {col}")
        
        # 转换为numpy数组
        open_price = data['开盘'].values
        high_price = data['最高'].values
        low_price = data['最低'].values
        close_price = data['收盘'].values
        volume = data['成交量'].values
        
        return open_price, high_price, low_price, close_price, volume
    
    # ===== 移动平均线指标 =====
    
    def ma(self, data: pd.DataFrame, timeperiod: int = 30) -> pd.Series:
        """
        计算简单移动平均线 (Simple Moving Average)
        
        Args:
            data: 股票数据
            timeperiod: 周期，默认30
            
        Returns:
            pd.Series: MA值
        """
        try:
            _, _, _, close, _ = self._prepare_ohlcv(data)
            result = talib.MA(close, timeperiod=timeperiod)
            return pd.Series(result, index=data.index, name=f'MA{timeperiod}')
        except Exception as e:
            logger.error(f"计算MA指标失败: {str(e)}")
            return pd.Series()
    
    def ema(self, data: pd.DataFrame, timeperiod: int = 30) -> pd.Series:
        """
        计算指数移动平均线 (Exponential Moving Average)
        
        Args:
            data: 股票数据
            timeperiod: 周期，默认30
            
        Returns:
            pd.Series: EMA值
        """
        try:
            _, _, _, close, _ = self._prepare_ohlcv(data)
            result = talib.EMA(close, timeperiod=timeperiod)
            return pd.Series(result, index=data.index, name=f'EMA{timeperiod}')
        except Exception as e:
            logger.error(f"计算EMA指标失败: {str(e)}")
            return pd.Series()
    
    # ===== 震荡指标 =====
    
    def macd(self, data: pd.DataFrame, fastperiod: int = 12, slowperiod: int = 26, signalperiod: int = 9) -> pd.DataFrame:
        """
        计算MACD指标 (Moving Average Convergence/Divergence)
        
        Args:
            data: 股票数据
            fastperiod: 快线周期，默认12
            slowperiod: 慢线周期，默认26
            signalperiod: 信号线周期，默认9
            
        Returns:
            pd.DataFrame: 包含MACD, Signal和Histogram的DataFrame
        """
        try:
            _, _, _, close, _ = self._prepare_ohlcv(data)
            macd, signal, hist = talib.MACD(close, fastperiod=fastperiod, slowperiod=slowperiod, signalperiod=signalperiod)
            
            result = pd.DataFrame({
                'MACD': macd,
                'Signal': signal,
                'Histogram': hist
            }, index=data.index)
            
            return result
        except Exception as e:
            logger.error(f"计算MACD指标失败: {str(e)}")
            return pd.DataFrame()
    
    def rsi(self, data: pd.DataFrame, timeperiod: int = 14) -> pd.Series:
        """
        计算相对强弱指标 (Relative Strength Index)
        
        Args:
            data: 股票数据
            timeperiod: 周期，默认14
            
        Returns:
            pd.Series: RSI值
        """
        try:
            _, _, _, close, _ = self._prepare_ohlcv(data)
            result = talib.RSI(close, timeperiod=timeperiod)
            return pd.Series(result, index=data.index, name=f'RSI{timeperiod}')
        except Exception as e:
            logger.error(f"计算RSI指标失败: {str(e)}")
            return pd.Series()
    
    def kdj(self, data: pd.DataFrame, fastk_period: int = 9, slowk_period: int = 3, slowd_period: int = 3) -> pd.DataFrame:
        """
        计算KDJ指标
        
        Args:
            data: 股票数据
            fastk_period: 计算K值的周期，默认9
            slowk_period: 计算慢速K值的周期，默认3
            slowd_period: 计算D值的周期，默认3
            
        Returns:
            pd.DataFrame: 包含K, D, J值的DataFrame
        """
        try:
            _, high, low, close, _ = self._prepare_ohlcv(data)
            k, d = talib.STOCH(high, low, close, fastk_period=fastk_period, slowk_period=slowk_period, 
                               slowk_matype=0, slowd_period=slowd_period, slowd_matype=0)
            
            # 计算J值: 3*K-2*D
            j = 3 * k - 2 * d
            
            result = pd.DataFrame({
                'K': k,
                'D': d,
                'J': j
            }, index=data.index)
            
            return result
        except Exception as e:
            logger.error(f"计算KDJ指标失败: {str(e)}")
            return pd.DataFrame()
    
    # ===== 趋势指标 =====
    
    def adx(self, data: pd.DataFrame, timeperiod: int = 14) -> pd.Series:
        """
        计算平均趋向指标 (Average Directional Movement Index)
        
        Args:
            data: 股票数据
            timeperiod: 周期，默认14
            
        Returns:
            pd.Series: ADX值
        """
        try:
            _, high, low, close, _ = self._prepare_ohlcv(data)
            result = talib.ADX(high, low, close, timeperiod=timeperiod)
            return pd.Series(result, index=data.index, name=f'ADX{timeperiod}')
        except Exception as e:
            logger.error(f"计算ADX指标失败: {str(e)}")
            return pd.Series()
    
    # ===== 波动指标 =====
    
    def bollinger_bands(self, data: pd.DataFrame, timeperiod: int = 20, nbdevup: float = 2.0, nbdevdn: float = 2.0) -> pd.DataFrame:
        """
        计算布林带 (Bollinger Bands)
        
        Args:
            data: 股票数据
            timeperiod: 周期，默认20
            nbdevup: 上轨标准差倍数，默认2.0
            nbdevdn: 下轨标准差倍数，默认2.0
            
        Returns:
            pd.DataFrame: 包含上轨、中轨、下轨的DataFrame
        """
        try:
            _, _, _, close, _ = self._prepare_ohlcv(data)
            upper, middle, lower = talib.BBANDS(close, timeperiod=timeperiod, nbdevup=nbdevup, nbdevdn=nbdevdn)
            
            result = pd.DataFrame({
                'Upper': upper,
                'Middle': middle,
                'Lower': lower
            }, index=data.index)
            
            return result
        except Exception as e:
            logger.error(f"计算布林带指标失败: {str(e)}")
            return pd.DataFrame()
    
    def atr(self, data: pd.DataFrame, timeperiod: int = 14) -> pd.Series:
        """
        计算平均真实范围 (Average True Range)
        
        Args:
            data: 股票数据
            timeperiod: 周期，默认14
            
        Returns:
            pd.Series: ATR值
        """
        try:
            _, high, low, close, _ = self._prepare_ohlcv(data)
            result = talib.ATR(high, low, close, timeperiod=timeperiod)
            return pd.Series(result, index=data.index, name=f'ATR{timeperiod}')
        except Exception as e:
            logger.error(f"计算ATR指标失败: {str(e)}")
            return pd.Series()
    
    # ===== 成交量指标 =====
    
    def obv(self, data: pd.DataFrame) -> pd.Series:
        """
        计算能量潮 (On Balance Volume)
        
        Args:
            data: 股票数据
            
        Returns:
            pd.Series: OBV值
        """
        try:
            _, _, _, close, volume = self._prepare_ohlcv(data)
            result = talib.OBV(close, volume)
            return pd.Series(result, index=data.index, name='OBV')
        except Exception as e:
            logger.error(f"计算OBV指标失败: {str(e)}")
            return pd.Series()
    
    # ===== 批量计算指标 =====
    
    def calculate_indicators(self, data: pd.DataFrame, indicators: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        批量计算多个技术指标
        
        Args:
            data: 股票数据
            indicators: 指标配置列表，每个元素为字典，包含指标名称和参数
                例如: [{"name": "ma", "params": {"timeperiod": 5}}, 
                      {"name": "rsi", "params": {"timeperiod": 14}}]
            
        Returns:
            pd.DataFrame: 包含所有计算结果的DataFrame
        """
        if data.empty:
            return pd.DataFrame()
        
        results = []
        
        for indicator_config in indicators:
            name = indicator_config.get("name", "")
            params = indicator_config.get("params", {})
            
            if not hasattr(self, name):
                logger.warning(f"未找到指标: {name}")
                continue
                
            # 调用对应的指标计算方法
            indicator_func = getattr(self, name)
            result = indicator_func(data, **params)
            
            if isinstance(result, pd.DataFrame):
                # 如果结果是DataFrame，添加前缀
                result = result.add_prefix(f"{name}_")
                results.append(result)
            elif isinstance(result, pd.Series):
                # 如果结果是Series，转换为DataFrame
                results.append(pd.DataFrame({result.name: result}))
        
        if not results:
            return pd.DataFrame()
            
        # 合并所有结果
        return pd.concat(results, axis=1)


# 创建全局实例
ta = TAIndicator()


# 便捷函数，直接使用全局实例
def get_stock_data(stock_code: str, start_date: str = None, end_date: str = None, 
                  period: str = DEFAULT_PERIOD, fields: List[str] = None) -> pd.DataFrame:
    """
    获取单只股票的历史数据
    """
    return ta.get_stock_data(stock_code, start_date, end_date, period, fields)


def get_multi_stock_data(stock_codes: List[str], start_date: str = None, end_date: str = None,
                        period: str = DEFAULT_PERIOD, fields: List[str] = None) -> Dict[str, pd.DataFrame]:
    """
    批量获取多只股票的历史数据
    """
    return ta.get_multi_stock_data(stock_codes, start_date, end_date, period, fields)


def calculate_indicators(data: pd.DataFrame, indicators: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    批量计算多个技术指标
    """
    return ta.calculate_indicators(data, indicators)


# 单个指标计算的便捷函数
def ma(data: pd.DataFrame, timeperiod: int = 30) -> pd.Series:
    return ta.ma(data, timeperiod)


def ema(data: pd.DataFrame, timeperiod: int = 30) -> pd.Series:
    return ta.ema(data, timeperiod)


def macd(data: pd.DataFrame, fastperiod: int = 12, slowperiod: int = 26, signalperiod: int = 9) -> pd.DataFrame:
    return ta.macd(data, fastperiod, slowperiod, signalperiod)


def rsi(data: pd.DataFrame, timeperiod: int = 14) -> pd.Series:
    return ta.rsi(data, timeperiod)


def kdj(data: pd.DataFrame, fastk_period: int = 9, slowk_period: int = 3, slowd_period: int = 3) -> pd.DataFrame:
    return ta.kdj(data, fastk_period, slowk_period, slowd_period)


def adx(data: pd.DataFrame, timeperiod: int = 14) -> pd.Series:
    return ta.adx(data, timeperiod)


def bollinger_bands(data: pd.DataFrame, timeperiod: int = 20, nbdevup: float = 2.0, nbdevdn: float = 2.0) -> pd.DataFrame:
    return ta.bollinger_bands(data, timeperiod, nbdevup, nbdevdn)


def atr(data: pd.DataFrame, timeperiod: int = 14) -> pd.Series:
    return ta.atr(data, timeperiod)


def obv(data: pd.DataFrame) -> pd.Series:
    return ta.obv(data)