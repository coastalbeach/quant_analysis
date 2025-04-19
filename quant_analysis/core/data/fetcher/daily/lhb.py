#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
龙虎榜数据获取模块

功能：
1. 获取东方财富龙虎榜详情数据
2. 获取东方财富个股上榜统计数据
3. 获取东方财富机构买卖每日统计数据
4. 获取东方财富机构席位追踪数据
5. 获取东方财富每日活跃营业部数据
6. 获取东方财富营业部排行数据
7. 获取东方财富营业部统计数据
8. 获取东方财富个股龙虎榜详情数据

数据存储策略：
- 使用PostgreSQL分区表优化存储结构，按日期范围分区
- 使用UPSERT功能处理主键冲突，确保数据更新而非插入失败
- 历史数据类接口（如龙虎榜详情、个股上榜统计等）：存入分区表
- 实时查询类接口（如个股龙虎榜详情）：封装为功能随用随调
"""

import logging
import datetime
import pandas as pd
import akshare as ak
import warnings
from typing import Dict, List, Optional, Union, Tuple
from core.storage import storage

# 过滤pandas FutureWarning警告
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas.core.reshape.concat")

# 初始化日志
logger = logging.getLogger('quant.fetcher.daily.lhb')


# =============== 数据获取函数 ===============

def fetch_lhb_detail(start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取东方财富龙虎榜详情数据
    
    接口: stock_lhb_detail_em
    数据源: 东方财富网-数据中心-龙虎榜单-龙虎榜详情
    
    Args:
        start_date: 开始日期，格式为YYYYMMDD
        end_date: 结束日期，格式为YYYYMMDD
        
    Returns:
        pandas.DataFrame: 龙虎榜详情数据
    """
    try:
        logger.info(f"开始获取龙虎榜详情数据，日期范围：{start_date}至{end_date}")
        df = ak.stock_lhb_detail_em(start_date=start_date, end_date=end_date)
        logger.info(f"成功获取龙虎榜详情数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取龙虎榜详情数据失败: {str(e)}")
        raise


def fetch_lhb_stock_statistic(period: str = "近一月") -> pd.DataFrame:
    """
    获取东方财富个股上榜统计数据
    
    接口: stock_lhb_stock_statistic_em
    数据源: 东方财富网-数据中心-龙虎榜单-个股上榜统计
    
    Args:
        period: 统计周期，可选值为"近一月"、"近三月"、"近六月"、"近一年"
        
    Returns:
        pandas.DataFrame: 个股上榜统计数据
    """
    try:
        logger.info(f"开始获取个股上榜统计数据，周期：{period}")
        df = ak.stock_lhb_stock_statistic_em(symbol=period)
        logger.info(f"成功获取个股上榜统计数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取个股上榜统计数据失败: {str(e)}")
        raise


def fetch_lhb_jgmmtj(start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取东方财富机构买卖每日统计数据
    
    接口: stock_lhb_jgmmtj_em
    数据源: 东方财富网-数据中心-龙虎榜单-机构买卖每日统计
    
    Args:
        start_date: 开始日期，格式为YYYYMMDD
        end_date: 结束日期，格式为YYYYMMDD
        
    Returns:
        pandas.DataFrame: 机构买卖每日统计数据
    """
    try:
        logger.info(f"开始获取机构买卖每日统计数据，日期范围：{start_date}至{end_date}")
        df = ak.stock_lhb_jgmmtj_em(start_date=start_date, end_date=end_date)
        logger.info(f"成功获取机构买卖每日统计数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取机构买卖每日统计数据失败: {str(e)}")
        raise


def fetch_lhb_jgstatistic(period: str = "近一月") -> pd.DataFrame:
    """
    获取东方财富机构席位追踪数据
    
    接口: stock_lhb_jgstatistic_em
    数据源: 东方财富网-数据中心-龙虎榜单-机构席位追踪
    
    Args:
        period: 统计周期，可选值为"近一月"、"近三月"、"近六月"、"近一年"
        
    Returns:
        pandas.DataFrame: 机构席位追踪数据
    """
    try:
        logger.info(f"开始获取机构席位追踪数据，周期：{period}")
        df = ak.stock_lhb_jgstatistic_em(symbol=period)
        logger.info(f"成功获取机构席位追踪数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取机构席位追踪数据失败: {str(e)}")
        raise


def fetch_lhb_hyyyb(start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取东方财富每日活跃营业部数据
    
    接口: stock_lhb_hyyyb_em
    数据源: 东方财富网-数据中心-龙虎榜单-每日活跃营业部
    
    Args:
        start_date: 开始日期，格式为YYYYMMDD
        end_date: 结束日期，格式为YYYYMMDD
        
    Returns:
        pandas.DataFrame: 每日活跃营业部数据
    """
    try:
        logger.info(f"开始获取每日活跃营业部数据，日期范围：{start_date}至{end_date}")
        df = ak.stock_lhb_hyyyb_em(start_date=start_date, end_date=end_date)
        logger.info(f"成功获取每日活跃营业部数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取每日活跃营业部数据失败: {str(e)}")
        raise


def fetch_lhb_yybph(period: str = "近一月") -> pd.DataFrame:
    """
    获取东方财富营业部排行数据
    
    接口: stock_lhb_yybph_em
    数据源: 东方财富网-数据中心-龙虎榜单-营业部排行
    
    Args:
        period: 统计周期，可选值为"近一月"、"近三月"、"近六月"、"近一年"
        
    Returns:
        pandas.DataFrame: 营业部排行数据
    """
    try:
        logger.info(f"开始获取营业部排行数据，周期：{period}")
        df = ak.stock_lhb_yybph_em(symbol=period)
        logger.info(f"成功获取营业部排行数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取营业部排行数据失败: {str(e)}")
        raise


def fetch_lhb_traderstatistic(period: str = "近一月") -> pd.DataFrame:
    """
    获取东方财富营业部统计数据
    
    接口: stock_lhb_traderstatistic_em
    数据源: 东方财富网-数据中心-龙虎榜单-营业部统计
    
    Args:
        period: 统计周期，可选值为"近一月"、"近三月"、"近六月"、"近一年"
        
    Returns:
        pandas.DataFrame: 营业部统计数据
    """
    try:
        logger.info(f"开始获取营业部统计数据，周期：{period}")
        df = ak.stock_lhb_traderstatistic_em(symbol=period)
        logger.info(f"成功获取营业部统计数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取营业部统计数据失败: {str(e)}")
        raise


def fetch_lhb_stock_detail(symbol: str, date: str, flag: str = "买入") -> pd.DataFrame:
    """
    获取东方财富个股龙虎榜详情数据
    
    接口: stock_lhb_stock_detail_em
    数据源: 东方财富网-数据中心-龙虎榜单-个股龙虎榜详情
    
    Args:
        symbol: 股票代码，如"600077"
        date: 日期，格式为YYYYMMDD
        flag: 买入或卖出，可选值为"买入"、"卖出"
        
    Returns:
        pandas.DataFrame: 个股龙虎榜详情数据
    """
    try:
        logger.info(f"开始获取个股龙虎榜详情数据，股票代码：{symbol}，日期：{date}，类型：{flag}")
        df = ak.stock_lhb_stock_detail_em(symbol=symbol, date=date, flag=flag)
        logger.info(f"成功获取个股龙虎榜详情数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取个股龙虎榜详情数据失败: {str(e)}")
        raise


# =============== 数据保存函数 ===============

def save_lhb_detail(start_date: str = None, end_date: str = None) -> None:
    """
    获取并保存龙虎榜详情数据到数据库
    
    数据表: 龙虎榜详情
    主键: ['代码', '上榜日']
    分区: 按上榜日期范围分区
    
    Args:
        start_date: 开始日期，格式为YYYYMMDD，默认为昨天
        end_date: 结束日期，格式为YYYYMMDD，默认为昨天
    """
    try:
        # 如果未指定日期，默认获取昨天的数据
        if start_date is None or end_date is None:
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
            start_date = start_date or yesterday
            end_date = end_date or yesterday
        
        # 获取数据
        df = fetch_lhb_detail(start_date=start_date, end_date=end_date)
        
        # 添加年月字段用于分区
        if not df.empty and '上榜日' in df.columns:
            # 确保上榜日是日期类型
            df['上榜日'] = pd.to_datetime(df['上榜日'])
            # 添加年月字段用于分区
            df['年月'] = df['上榜日'].dt.strftime('%Y%m')
        
        # 保存数据 - 使用UPSERT模式处理主键冲突
        storage.save(
            df=df,
            data_type="龙虎榜详情",
            primary_keys=["代码", "上榜日"],
            replace=False,  # 使用ON CONFLICT更新而非替换
            partition_by="年月"  # 按年月分区
        )
        logger.info(f"龙虎榜详情数据保存成功，日期范围：{start_date}至{end_date}，共{len(df)}条记录")
    except Exception as e:
        logger.error(f"龙虎榜详情数据保存失败: {str(e)}")
        raise


def save_lhb_stock_statistic(period: str = "近一月") -> None:
    """
    获取并保存个股上榜统计数据到数据库
    
    数据表: 个股上榜统计
    主键: ['代码', '统计周期']
    
    Args:
        period: 统计周期，可选值为"近一月"、"近三月"、"近六月"、"近一年"
    """
    try:
        # 获取数据
        df = fetch_lhb_stock_statistic(period=period)
        
        # 添加统计周期字段
        df['统计周期'] = period
        
        # 保存数据
        storage.save(
            df=df,
            data_type="个股上榜统计",
            primary_keys=["代码", "统计周期"],
            replace=False,  # 使用ON CONFLICT更新而非替换
            upsert_fields=["上榜次数", "上榜涨幅", "上榜跌幅", "累计涨幅", "平均涨幅", "最近上榜日"]  # 冲突时更新这些字段
        )
        logger.info(f"个股上榜统计数据({period})保存成功")
    except Exception as e:
        logger.error(f"个股上榜统计数据({period})保存失败: {str(e)}")
        raise


def save_lhb_jgmmtj(start_date: str = None, end_date: str = None) -> None:
    """
    获取并保存机构买卖每日统计数据到数据库
    
    数据表: 机构买卖每日统计
    主键: ['代码', '上榜日期']
    分区: 按上榜日期范围分区
    
    Args:
        start_date: 开始日期，格式为YYYYMMDD，默认为昨天
        end_date: 结束日期，格式为YYYYMMDD，默认为昨天
    """
    try:
        # 如果未指定日期，默认获取昨天的数据
        if start_date is None or end_date is None:
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
            start_date = start_date or yesterday
            end_date = end_date or yesterday
        
        # 获取数据
        df = fetch_lhb_jgmmtj(start_date=start_date, end_date=end_date)
        
        # 添加年月字段用于分区
        if not df.empty and '上榜日期' in df.columns:
            # 确保上榜日期是日期类型
            df['上榜日期'] = pd.to_datetime(df['上榜日期'])
            # 添加年月字段用于分区
            df['年月'] = df['上榜日期'].dt.strftime('%Y%m')
        
        # 保存数据 - 使用UPSERT模式处理主键冲突
        storage.save(
            df=df,
            data_type="机构买卖每日统计",
            primary_keys=["代码", "上榜日期"],
            replace=False,  # 使用ON CONFLICT更新而非替换
            partition_by="年月"  # 按年月分区
        )
        logger.info(f"机构买卖每日统计数据保存成功，日期范围：{start_date}至{end_date}，共{len(df)}条记录")
    except Exception as e:
        logger.error(f"机构买卖每日统计数据保存失败: {str(e)}")
        raise


def save_lhb_jgstatistic(period: str = "近一月") -> None:
    """
    获取并保存机构席位追踪数据到数据库
    
    数据表: 机构席位追踪
    主键: ['代码', '统计周期']
    
    Args:
        period: 统计周期，可选值为"近一月"、"近三月"、"近六月"、"近一年"
    """
    try:
        # 获取数据
        df = fetch_lhb_jgstatistic(period=period)
        
        # 添加统计周期字段
        df['统计周期'] = period
        
        # 保存数据
        storage.save(
            df=df,
            data_type="机构席位追踪",
            primary_keys=["代码", "统计周期"],
            replace=False,  # 使用ON CONFLICT更新而非替换
            upsert_fields=["买入次数", "买入金额", "卖出次数", "卖出金额", "净额", "最近上榜日"]  # 冲突时更新这些字段
        )
        logger.info(f"机构席位追踪数据({period})保存成功")
    except Exception as e:
        logger.error(f"机构席位追踪数据({period})保存失败: {str(e)}")
        raise


def save_lhb_hyyyb(start_date: str = None, end_date: str = None) -> None:
    """
    获取并保存每日活跃营业部数据到数据库
    
    数据表: 每日活跃营业部
    主键: ['营业部名称', '上榜日']
    分区: 按上榜日期范围分区
    
    Args:
        start_date: 开始日期，格式为YYYYMMDD，默认为昨天
        end_date: 结束日期，格式为YYYYMMDD，默认为昨天
    """
    try:
        # 如果未指定日期，默认获取昨天的数据
        if start_date is None or end_date is None:
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
            start_date = start_date or yesterday
            end_date = end_date or yesterday
        
        # 获取数据
        df = fetch_lhb_hyyyb(start_date=start_date, end_date=end_date)
        
        # 添加年月字段用于分区
        if not df.empty and '上榜日' in df.columns:
            # 确保上榜日是日期类型
            df['上榜日'] = pd.to_datetime(df['上榜日'])
            # 添加年月字段用于分区
            df['年月'] = df['上榜日'].dt.strftime('%Y%m')
        
        # 保存数据
        storage.save(
            df=df,
            data_type="每日活跃营业部",
            primary_keys=["营业部名称", "上榜日"],
            replace=False,  # 使用ON CONFLICT更新而非替换
            partition_by="年月"  # 按年月分区
        )
        logger.info(f"每日活跃营业部数据保存成功，日期范围：{start_date}至{end_date}，共{len(df)}条记录")
    except Exception as e:
        logger.error(f"每日活跃营业部数据保存失败: {str(e)}")
        raise


def save_lhb_yybph(period: str = "近一月") -> None:
    """
    获取并保存营业部排行数据到数据库
    
    数据表: 营业部排行
    主键: ['营业部名称', '统计周期']
    
    Args:
        period: 统计周期，可选值为"近一月"、"近三月"、"近六月"、"近一年"
    """
    try:
        # 获取数据
        df = fetch_lhb_yybph(period=period)
        
        # 添加统计周期字段
        df['统计周期'] = period
        
        # 保存数据
        storage.save(
            df=df,
            data_type="营业部排行",
            primary_keys=["营业部名称", "统计周期"],
            replace=False,  # 使用ON CONFLICT更新而非替换
            upsert_fields=["上榜次数", "买入金额", "卖出金额", "买入次数", "卖出次数", "净额"]  # 冲突时更新这些字段
        )
        logger.info(f"营业部排行数据({period})保存成功")
    except Exception as e:
        logger.error(f"营业部排行数据({period})保存失败: {str(e)}")
        raise


def save_lhb_traderstatistic(period: str = "近一月") -> None:
    """
    获取并保存营业部统计数据到数据库
    
    数据表: 营业部统计
    主键: ['营业部名称', '统计周期']
    
    Args:
        period: 统计周期，可选值为"近一月"、"近三月"、"近六月"、"近一年"
    """
    try:
        # 获取数据
        df = fetch_lhb_traderstatistic(period=period)
        
        # 添加统计周期字段
        df['统计周期'] = period
        
        # 保存数据
        storage.save(
            df=df,
            data_type="营业部统计",
            primary_keys=["营业部名称", "统计周期"],
            replace=False,  # 使用ON CONFLICT更新而非替换
            upsert_fields=["上榜次数", "累计买入额", "买入次数", "累计卖出额", "卖出次数", "净额"]  # 冲突时更新这些字段
        )
        logger.info(f"营业部统计数据({period})保存成功")
    except Exception as e:
        logger.error(f"营业部统计数据({period})保存失败: {str(e)}")
        raise


# =============== 功能接口函数 ===============

def get_stock_lhb_detail(symbol: str, date: str, flag: str = "买入") -> pd.DataFrame:
    """
    获取个股龙虎榜详情数据（随用随调）
    
    Args:
        symbol: 股票代码，如"600077"
        date: 日期，格式为YYYYMMDD
        flag: 买入或卖出，可选值为"买入"、"卖出"
        
    Returns:
        pandas.DataFrame: 个股龙虎榜详情数据
    """
    return fetch_lhb_stock_detail(symbol=symbol, date=date, flag=flag)


def update_all_lhb_data(start_date: str = None, end_date: str = None) -> None:
    """
    更新所有龙虎榜数据
    
    Args:
        start_date: 开始日期，格式为YYYYMMDD，默认为昨天
        end_date: 结束日期，格式为YYYYMMDD，默认为昨天
    """
    try:
        # 如果未指定日期，默认获取昨天的数据
        if start_date is None or end_date is None:
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
            start_date = start_date or yesterday
            end_date = end_date or yesterday
        
        # 更新需要按日期范围获取的数据
        save_lhb_detail(start_date=start_date, end_date=end_date)
        save_lhb_jgmmtj(start_date=start_date, end_date=end_date)
        save_lhb_hyyyb(start_date=start_date, end_date=end_date)
        
        # 更新统计类数据
        periods = ["近一月", "近三月", "近六月", "近一年"]
        for period in periods:
            save_lhb_stock_statistic(period=period)
            save_lhb_jgstatistic(period=period)
            save_lhb_yybph(period=period)
            save_lhb_traderstatistic(period=period)
        
        logger.info("所有龙虎榜数据更新成功")
    except Exception as e:
        logger.error(f"龙虎榜数据更新失败: {str(e)}")
        raise


# 如果直接运行此模块，则更新所有龙虎榜数据
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 更新所有龙虎榜数据
    update_all_lhb_data( )