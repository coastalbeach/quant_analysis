#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
龙虎榜数据模块

本模块整合了龙虎榜数据的获取、存储和分析功能，提供统一的接口供其他模块调用。

功能：
1. 提供龙虎榜数据的获取和存储功能
2. 提供龙虎榜数据的分析功能
3. 提供龙虎榜数据的查询功能

使用方法：
```python
# 更新龙虎榜数据
from core.data.fetcher.lhb import update_lhb_data
update_lhb_data()

# 分析龙虎榜数据
from core.data.fetcher.lhb import analyze_hot_stocks
hot_stocks = analyze_hot_stocks(days=30)
```
"""

import logging
import datetime
from typing import Dict, List, Optional, Union, Tuple

# 导入子模块
from core.data.fetcher.daily.lhb import (
    # 数据获取函数
    fetch_lhb_detail,
    fetch_lhb_stock_statistic,
    fetch_lhb_jgmmtj,
    fetch_lhb_jgstatistic,
    fetch_lhb_hyyyb,
    fetch_lhb_yybph,
    fetch_lhb_traderstatistic,
    fetch_lhb_stock_detail,
    
    # 数据保存函数
    save_lhb_detail,
    save_lhb_stock_statistic,
    save_lhb_jgmmtj,
    save_lhb_jgstatistic,
    save_lhb_hyyyb,
    save_lhb_yybph,
    save_lhb_traderstatistic,
    
    # 功能接口函数
    get_stock_lhb_detail,
    update_all_lhb_data
)

# 导入分析模块
from core.data.fetcher.analyst.lhb_analyst import (
    analyze_hot_stocks,
    analyze_institution_behavior,
    analyze_active_traders,
    analyze_stock_performance_after_lhb,
    get_lhb_stock_selection,
    get_lhb_summary
)

# 初始化日志
logger = logging.getLogger('quant.fetcher.lhb')


# =============== 对外接口函数 ===============

def update_lhb_data(start_date: str = None, end_date: str = None, update_all: bool = True) -> None:
    """
    更新龙虎榜数据
    
    Args:
        start_date: 开始日期，格式为YYYYMMDD，默认为昨天
        end_date: 结束日期，格式为YYYYMMDD，默认为昨天
        update_all: 是否更新所有类型的龙虎榜数据，默认为True
    """
    if update_all:
        update_all_lhb_data(start_date=start_date, end_date=end_date)
    else:
        # 如果未指定日期，默认获取昨天的数据
        if start_date is None or end_date is None:
            yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
            start_date = start_date or yesterday
            end_date = end_date or yesterday
        
        # 只更新基础数据
        save_lhb_detail(start_date=start_date, end_date=end_date)
        save_lhb_jgmmtj(start_date=start_date, end_date=end_date)
        save_lhb_hyyyb(start_date=start_date, end_date=end_date)


def get_lhb_data(data_type: str, **kwargs) -> Dict:
    """
    获取龙虎榜数据
    
    Args:
        data_type: 数据类型，可选值为：
            - "hot_stocks": 热门股票
            - "institution_behavior": 机构买卖行为
            - "active_traders": 活跃营业部
            - "stock_performance": 个股上榜后表现
            - "stock_selection": 龙虎榜选股
            - "summary": 龙虎榜数据摘要
            - "stock_detail": 个股龙虎榜详情
        **kwargs: 其他参数，根据数据类型不同而不同
    
    Returns:
        Dict: 龙虎榜数据
    """
    try:
        if data_type == "hot_stocks":
            days = kwargs.get("days", 30)
            min_count = kwargs.get("min_count", 2)
            return analyze_hot_stocks(days=days, min_count=min_count).to_dict("records")
        
        elif data_type == "institution_behavior":
            days = kwargs.get("days", 30)
            top_n = kwargs.get("top_n", 20)
            return analyze_institution_behavior(days=days, top_n=top_n).to_dict("records")
        
        elif data_type == "active_traders":
            days = kwargs.get("days", 30)
            top_n = kwargs.get("top_n", 20)
            return analyze_active_traders(days=days, top_n=top_n).to_dict("records")
        
        elif data_type == "stock_performance":
            days = kwargs.get("days", 90)
            return analyze_stock_performance_after_lhb(days=days).to_dict("records")
        
        elif data_type == "stock_selection":
            days = kwargs.get("days", 30)
            min_count = kwargs.get("min_count", 2)
            min_inst_buy = kwargs.get("min_inst_buy", 1000000)
            return get_lhb_stock_selection(days=days, min_count=min_count, min_inst_buy=min_inst_buy).to_dict("records")
        
        elif data_type == "summary":
            days = kwargs.get("days", 30)
            return get_lhb_summary(days=days)
        
        elif data_type == "stock_detail":
            symbol = kwargs.get("symbol")
            date = kwargs.get("date")
            flag = kwargs.get("flag", "买入")
            if not symbol or not date:
                raise ValueError("获取个股龙虎榜详情需要提供股票代码和日期")
            return get_stock_lhb_detail(symbol=symbol, date=date, flag=flag).to_dict("records")
        
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")
    
    except Exception as e:
        logger.error(f"获取龙虎榜数据失败: {str(e)}")
        raise


# 如果直接运行此模块，则更新所有龙虎榜数据
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 更新所有龙虎榜数据
    update_lhb_data()
    
    # 获取龙虎榜数据摘要
    summary = get_lhb_data("summary", days=30)
    print("\n龙虎榜数据摘要:")
    for k, v in summary.items():
        print(f"{k}: {v}")
    
    # 获取热门股票
    hot_stocks = get_lhb_data("hot_stocks", days=30, min_count=2)
    print("\n热门上榜股票:")
    for stock in hot_stocks[:10]:
        print(f"{stock['代码']} {stock['名称']} 上榜次数: {stock['上榜次数']} 平均5日涨跌幅: {stock['平均5日涨跌幅']}%")