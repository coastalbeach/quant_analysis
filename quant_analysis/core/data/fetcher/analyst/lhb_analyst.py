#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
龙虎榜数据分析模块

功能：
1. 分析龙虎榜热门股票
2. 分析机构买卖行为
3. 分析营业部活跃度
4. 分析个股上榜后的表现
5. 提供龙虎榜选股策略

本模块基于龙虎榜数据，提供各种分析功能，不进行数据存储，而是直接从数据库读取数据进行分析。
"""

import logging
import datetime
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Tuple
from core.storage import storage

# 初始化日志
logger = logging.getLogger('quant.analyst.lhb')


# =============== 数据分析函数 ===============

def analyze_hot_stocks(days: int = 30, min_count: int = 2) -> pd.DataFrame:
    """
    分析龙虎榜热门股票
    
    查询最近一段时间内多次上榜的股票，并计算上榜后的平均涨跌幅
    
    Args:
        days: 分析的时间范围，单位为天，默认为30天
        min_count: 最小上榜次数，默认为2次
        
    Returns:
        pandas.DataFrame: 热门股票分析结果，包含股票代码、名称、上榜次数、上榜后平均涨跌幅等信息
    """
    try:
        # 计算日期范围
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        # 从数据库查询龙虎榜详情数据
        sql = f"""
        SELECT 代码, 名称, COUNT(*) AS 上榜次数, 
               AVG(上榜后1日) AS 平均1日涨跌幅,
               AVG(上榜后2日) AS 平均2日涨跌幅,
               AVG(上榜后5日) AS 平均5日涨跌幅,
               AVG(上榜后10日) AS 平均10日涨跌幅
        FROM 龙虎榜详情
        WHERE 上榜日 >= '{start_date_str}'
        GROUP BY 代码, 名称
        HAVING COUNT(*) >= {min_count}
        ORDER BY 上榜次数 DESC, 平均5日涨跌幅 DESC
        """
        
        result = storage.query(sql)
        
        # 转换为DataFrame
        df = pd.DataFrame(result)
        
        logger.info(f"成功分析热门股票，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"分析热门股票失败: {str(e)}")
        raise


def analyze_institution_behavior(days: int = 30, top_n: int = 20) -> pd.DataFrame:
    """
    分析机构买卖行为
    
    分析最近一段时间内机构买入和卖出最多的股票
    
    Args:
        days: 分析的时间范围，单位为天，默认为30天
        top_n: 返回前N只股票，默认为20只
        
    Returns:
        pandas.DataFrame: 机构买卖行为分析结果，包含股票代码、名称、机构买入金额、机构卖出金额、净买入金额等信息
    """
    try:
        # 计算日期范围
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        # 从数据库查询机构买卖每日统计数据
        sql = f"""
        SELECT 代码, 名称, 
               SUM(机构买入总额) AS 机构买入总额,
               SUM(机构卖出总额) AS 机构卖出总额,
               SUM(机构买入净额) AS 机构净买入总额,
               COUNT(*) AS 上榜次数
        FROM 机构买卖每日统计
        WHERE 上榜日期 >= '{start_date_str}'
        GROUP BY 代码, 名称
        ORDER BY 机构净买入总额 DESC
        LIMIT {top_n}
        """
        
        result = storage.query(sql)
        
        # 转换为DataFrame
        df = pd.DataFrame(result)
        
        # 计算买入占比和卖出占比
        if not df.empty:
            df['机构买入占比'] = df['机构买入总额'] / (df['机构买入总额'] + df['机构卖出总额']) * 100
            df['机构卖出占比'] = df['机构卖出总额'] / (df['机构买入总额'] + df['机构卖出总额']) * 100
        
        logger.info(f"成功分析机构买卖行为，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"分析机构买卖行为失败: {str(e)}")
        raise


def analyze_active_traders(days: int = 30, top_n: int = 20) -> pd.DataFrame:
    """
    分析活跃营业部
    
    分析最近一段时间内最活跃的营业部及其交易特点
    
    Args:
        days: 分析的时间范围，单位为天，默认为30天
        top_n: 返回前N个营业部，默认为20个
        
    Returns:
        pandas.DataFrame: 活跃营业部分析结果，包含营业部名称、上榜次数、买入金额、卖出金额、净买入金额等信息
    """
    try:
        # 计算日期范围
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        # 从数据库查询每日活跃营业部数据
        sql = f"""
        SELECT 营业部名称, 
               COUNT(*) AS 上榜次数,
               SUM(买入总金额) AS 买入总金额,
               SUM(卖出总金额) AS 卖出总金额,
               SUM(总买卖净额) AS 净买入总金额,
               SUM(买入个股数) AS 买入个股总数,
               SUM(卖出个股数) AS 卖出个股总数
        FROM 每日活跃营业部
        WHERE 上榜日 >= '{start_date_str}'
        GROUP BY 营业部名称
        ORDER BY 上榜次数 DESC
        LIMIT {top_n}
        """
        
        result = storage.query(sql)
        
        # 转换为DataFrame
        df = pd.DataFrame(result)
        
        # 计算买入占比和卖出占比
        if not df.empty:
            df['买入占比'] = df['买入总金额'] / (df['买入总金额'] + df['卖出总金额']) * 100
            df['卖出占比'] = df['卖出总金额'] / (df['买入总金额'] + df['卖出总金额']) * 100
            df['平均每股买入金额'] = df['买入总金额'] / df['买入个股总数']
            df['平均每股卖出金额'] = df['卖出总金额'] / df['卖出个股总数']
        
        logger.info(f"成功分析活跃营业部，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"分析活跃营业部失败: {str(e)}")
        raise


def analyze_stock_performance_after_lhb(days: int = 90) -> pd.DataFrame:
    """
    分析个股上榜后的表现
    
    分析个股上榜后1日、2日、5日、10日的平均涨跌幅，找出上榜后表现最好的股票
    
    Args:
        days: 分析的时间范围，单位为天，默认为90天
        
    Returns:
        pandas.DataFrame: 个股上榜后表现分析结果，包含股票代码、名称、上榜次数、上榜后平均涨跌幅等信息
    """
    try:
        # 计算日期范围
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        # 从数据库查询龙虎榜详情数据
        sql = f"""
        SELECT 代码, 名称, COUNT(*) AS 上榜次数, 
               AVG(上榜后1日) AS 平均1日涨跌幅,
               AVG(上榜后2日) AS 平均2日涨跌幅,
               AVG(上榜后5日) AS 平均5日涨跌幅,
               AVG(上榜后10日) AS 平均10日涨跌幅,
               MAX(上榜后10日) AS 最大10日涨跌幅,
               MIN(上榜后10日) AS 最小10日涨跌幅
        FROM 龙虎榜详情
        WHERE 上榜日 >= '{start_date_str}'
        GROUP BY 代码, 名称
        HAVING COUNT(*) >= 2
        ORDER BY 平均5日涨跌幅 DESC
        """
        
        result = storage.query(sql)
        
        # 转换为DataFrame
        df = pd.DataFrame(result)
        
        # 计算上榜后表现稳定性（标准差）
        if not df.empty:
            # 查询每只股票的上榜后涨跌幅详情
            stocks = df['代码'].tolist()
            placeholders = ",".join([f"'{s}'" for s in stocks])
            detail_sql = f"""
            SELECT 代码, 上榜后1日, 上榜后2日, 上榜后5日, 上榜后10日
            FROM 龙虎榜详情
            WHERE 代码 IN ({placeholders}) AND 上榜日 >= '{start_date_str}'
            """
            detail_result = storage.query(detail_sql)
            detail_df = pd.DataFrame(detail_result)
            
            # 计算每只股票上榜后涨跌幅的标准差
            std_df = detail_df.groupby('代码').agg({
                '上榜后1日': 'std',
                '上榜后2日': 'std',
                '上榜后5日': 'std',
                '上榜后10日': 'std'
            }).reset_index()
            std_df.columns = ['代码', '1日波动性', '2日波动性', '5日波动性', '10日波动性']
            
            # 合并结果
            df = pd.merge(df, std_df, on='代码', how='left')
            
            # 计算综合得分（平均涨幅 / 波动性）
            df['5日稳定性得分'] = df['平均5日涨跌幅'] / df['5日波动性']
            df['10日稳定性得分'] = df['平均10日涨跌幅'] / df['10日波动性']
        
        logger.info(f"成功分析个股上榜后表现，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"分析个股上榜后表现失败: {str(e)}")
        raise


def get_lhb_stock_selection(days: int = 30, min_count: int = 2, min_inst_buy: float = 1000000) -> pd.DataFrame:
    """
    龙虎榜选股策略
    
    基于龙虎榜数据的选股策略，选出同时满足以下条件的股票：
    1. 最近一段时间内多次上榜
    2. 机构净买入金额较大
    3. 上榜后表现较好
    
    Args:
        days: 分析的时间范围，单位为天，默认为30天
        min_count: 最小上榜次数，默认为2次
        min_inst_buy: 最小机构净买入金额，默认为100万元
        
    Returns:
        pandas.DataFrame: 选股结果，包含股票代码、名称、上榜次数、机构净买入金额、上榜后平均涨跌幅等信息
    """
    try:
        # 计算日期范围
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        # 从数据库查询龙虎榜详情和机构买卖每日统计数据
        sql = f"""
        WITH lhb AS (
            SELECT 代码, 名称, COUNT(*) AS 上榜次数, 
                   AVG(上榜后1日) AS 平均1日涨跌幅,
                   AVG(上榜后2日) AS 平均2日涨跌幅,
                   AVG(上榜后5日) AS 平均5日涨跌幅,
                   AVG(上榜后10日) AS 平均10日涨跌幅
            FROM 龙虎榜详情
            WHERE 上榜日 >= '{start_date_str}'
            GROUP BY 代码, 名称
            HAVING COUNT(*) >= {min_count}
        ),
        jg AS (
            SELECT 代码, SUM(机构买入净额) AS 机构净买入总额
            FROM 机构买卖每日统计
            WHERE 上榜日期 >= '{start_date_str}'
            GROUP BY 代码
            HAVING SUM(机构买入净额) >= {min_inst_buy}
        )
        SELECT l.代码, l.名称, l.上榜次数, 
               j.机构净买入总额,
               l.平均1日涨跌幅, l.平均2日涨跌幅, l.平均5日涨跌幅, l.平均10日涨跌幅
        FROM lhb l
        JOIN jg j ON l.代码 = j.代码
        ORDER BY j.机构净买入总额 DESC, l.平均5日涨跌幅 DESC
        """
        
        result = storage.query(sql)
        
        # 转换为DataFrame
        df = pd.DataFrame(result)
        
        # 计算综合得分
        if not df.empty:
            # 标准化处理
            df['上榜次数_norm'] = (df['上榜次数'] - df['上榜次数'].min()) / (df['上榜次数'].max() - df['上榜次数'].min())
            df['机构净买入_norm'] = (df['机构净买入总额'] - df['机构净买入总额'].min()) / (df['机构净买入总额'].max() - df['机构净买入总额'].min())
            df['5日涨幅_norm'] = (df['平均5日涨跌幅'] - df['平均5日涨跌幅'].min()) / (df['平均5日涨跌幅'].max() - df['平均5日涨跌幅'].min())
            
            # 计算综合得分（权重可调整）
            df['综合得分'] = df['上榜次数_norm'] * 0.3 + df['机构净买入_norm'] * 0.4 + df['5日涨幅_norm'] * 0.3
            
            # 按综合得分排序
            df = df.sort_values(by='综合得分', ascending=False)
        
        logger.info(f"成功执行龙虎榜选股策略，共选出 {len(df)} 只股票")
        return df
    except Exception as e:
        logger.error(f"龙虎榜选股策略执行失败: {str(e)}")
        raise


# =============== 辅助函数 ===============

def get_lhb_summary(days: int = 30) -> Dict:
    """
    获取龙虎榜数据摘要
    
    统计最近一段时间内的龙虎榜数据概况
    
    Args:
        days: 分析的时间范围，单位为天，默认为30天
        
    Returns:
        Dict: 龙虎榜数据摘要，包含上榜股票数、机构参与次数、平均涨跌幅等信息
    """
    try:
        # 计算日期范围
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        # 从数据库查询龙虎榜详情数据
        sql = f"""
        SELECT COUNT(DISTINCT 代码) AS 上榜股票数,
               COUNT(*) AS 上榜总次数,
               AVG(上榜后1日) AS 平均1日涨跌幅,
               AVG(上榜后5日) AS 平均5日涨跌幅,
               AVG(上榜后10日) AS 平均10日涨跌幅
        FROM 龙虎榜详情
        WHERE 上榜日 >= '{start_date_str}'
        """
        
        lhb_result = storage.query(sql)
        
        # 查询机构参与情况
        jg_sql = f"""
        SELECT COUNT(*) AS 机构参与次数,
               SUM(买方机构数) AS 买方机构总数,
               SUM(卖方机构数) AS 卖方机构总数,
               SUM(机构买入净额) AS 机构净买入总额
        FROM 机构买卖每日统计
        WHERE 上榜日期 >= '{start_date_str}'
        """
        
        jg_result = storage.query(jg_sql)
        
        # 合并结果
        summary = {}
        if lhb_result:
            summary.update(lhb_result[0])
        if jg_result:
            summary.update(jg_result[0])
            
        # 计算机构参与率
        if 'jg_result' in locals() and 'lhb_result' in locals() and lhb_result and jg_result:
            summary['机构参与率'] = jg_result[0]['机构参与次数'] / lhb_result[0]['上榜总次数'] * 100 if lhb_result[0]['上榜总次数'] > 0 else 0
        
        logger.info(f"成功获取龙虎榜数据摘要")
        return summary
    except Exception as e:
        logger.error(f"获取龙虎榜数据摘要失败: {str(e)}")
        raise


# 如果直接运行此模块，则执行示例分析
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 执行示例分析
    summary = get_lhb_summary(days=30)
    print("\n龙虎榜数据摘要:")
    for k, v in summary.items():
        print(f"{k}: {v}")
    
    hot_stocks = analyze_hot_stocks(days=30, min_count=2)
    print("\n热门上榜股票:")
    print(hot_stocks.head(10))
    
    inst_behavior = analyze_institution_behavior(days=30, top_n=10)
    print("\n机构买卖行为:")
    print(inst_behavior.head(10))
    
    stock_selection = get_lhb_stock_selection(days=30, min_count=2, min_inst_buy=1000000)
    print("\n龙虎榜选股结果:")
    print(stock_selection.head(10))