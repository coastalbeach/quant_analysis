#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
筹码分布数据获取模块

功能：
1. 获取东方财富网所有股票的筹码分布数据
2. 将数据保存到数据库

数据说明：
- 筹码分布数据反映了市场参与者的持仓成本分布
- 数据包含获利比例、平均成本、90/70成本区间等指标
- 默认获取不复权数据
"""

import logging
import pandas as pd
import akshare as ak
import concurrent.futures
from core.storage import storage

# 初始化日志
logger = logging.getLogger('quant.fetcher.daily.stock_cyq')


def fetch_stock_cyq(symbol: str) -> pd.DataFrame:
    """
    获取单只股票的筹码分布数据
    
    接口: stock_cyq_em
    数据源: 东方财富网-概念板-行情中心-日K-筹码分布
    
    Args:
        symbol: 股票代码，如 "000001"
        
    Returns:
        pandas.DataFrame: 筹码分布数据
    """
    try:
        df = ak.stock_cyq_em(symbol=symbol, adjust="")
        if df is not None and not df.empty:
            # 添加股票代码列
            df['股票代码'] = symbol
            return df
        else:
            logger.warning(f"股票 {symbol} 未获取到筹码分布数据")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"获取股票 {symbol} 筹码分布数据失败: {str(e)}")
        return pd.DataFrame()


def fetch_all_stock_cyq():
    """
    获取所有股票的筹码分布数据并存入数据库
    
    步骤：
    1. 获取所有股票列表
    2. 并行获取每只股票的筹码分布数据
    3. 将数据保存到数据库
    
    Returns:
        int: 成功获取的股票数量
    """
    try:
        # 获取所有股票列表
        logger.info("开始获取所有股票列表")
        stock_df = ak.stock_zh_a_spot_em()
        stock_list = stock_df['代码'].tolist()
        logger.info(f"成功获取股票列表，共 {len(stock_list)} 只股票")
        
        # 存储所有成功获取的数据
        all_data = []
        success_count = 0
        
        # 使用线程池并行获取数据
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # 提交所有任务
            future_to_symbol = {executor.submit(fetch_stock_cyq, symbol): symbol for symbol in stock_list}
            
            # 处理完成的任务
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    df = future.result()
                    if not df.empty:
                        all_data.append(df)
                        success_count += 1
                        if success_count % 50 == 0:
                            logger.info(f"已成功获取 {success_count} 只股票的筹码分布数据")
                except Exception as e:
                    logger.error(f"处理股票 {symbol} 的筹码分布数据时出错: {str(e)}")
        
        # 合并所有数据
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"成功获取 {success_count} 只股票的筹码分布数据，共 {len(combined_df)} 条记录")
            
            # 保存到数据库
            table_name = '筹码分布'
            storage.save_dataframe(combined_df, table_name, if_exists='replace')
            logger.info(f"筹码分布数据已保存到数据库表 {table_name}")
            
            return success_count
        else:
            logger.warning("未获取到任何筹码分布数据")
            return 0
    except Exception as e:
        logger.error(f"获取筹码分布数据过程中出错: {str(e)}")
        raise


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 执行数据获取
    fetch_all_stock_cyq()