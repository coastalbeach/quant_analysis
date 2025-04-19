#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票基础数据获取模块

功能：
1. 获取A股全部股票的基础信息
2. 根据股票代码判断市场类型
3. 根据流通市值划分市值等级
4. 获取股票所属行业板块
5. 获取股票所属概念板块
6. 将数据保存到数据库

优化：
1. 使用并行处理提高数据获取效率
2. 调整列排序满足业务需求
3. 完全重写数据表确保数据一致性
"""

import logging
import pandas as pd
import akshare as ak
import concurrent.futures
from core.storage import storage

# 初始化日志
logger = logging.getLogger('quant.fetcher.stock_bisic')


def fetch_all_stocks():
    """
    获取沪深京A股全部股票的基础信息
    
    接口: stock_zh_a_spot_em
    数据源: 东方财富网-沪深京A股-实时行情数据
    
    Returns:
        pandas.DataFrame: A股全部股票基础信息
    """
    try:
        logger.info("开始获取A股全部股票基础信息")
        df = ak.stock_zh_a_spot_em()
        # 只保留需要的字段
        df = df[['代码', '名称', '总市值', '流通市值', '60日涨跌幅']]
        logger.info(f"成功获取A股全部股票基础信息，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取A股全部股票基础信息失败: {str(e)}")
        raise


def determine_market(stock_code):
    """
    根据股票代码判断市场类型
    
    Args:
        stock_code (str): 股票代码
        
    Returns:
        str: 市场类型，包括沪市主板、深市主板、科创板、创业板、京市
    """
    prefix = stock_code[:3]
    
    if stock_code.startswith('688'):
        return '科创'
    elif stock_code.startswith('300') or stock_code.startswith('301'):
        return '创'
    elif stock_code.startswith('600') or stock_code.startswith('601') or stock_code.startswith('603') or stock_code.startswith('605'):
        return '沪A'
    elif stock_code.startswith('000') or stock_code.startswith('001') or stock_code.startswith('002') or stock_code.startswith('003'):
        return '深A'
    else:
        return '京A'


def determine_size_category(market_cap):
    """
    根据流通市值划分市值等级
    
    Args:
        market_cap (float): 流通市值（元）
        
    Returns:
        str: 市值等级，包括小盘股、中盘股、大盘股、巨头
    """
    # 转换为亿元
    market_cap_billion = market_cap / 100000000
    
    if market_cap_billion < 200:
        return '小盘股'
    elif market_cap_billion < 500:
        return '中盘股'
    elif market_cap_billion < 1200:
        return '大盘股'
    else:
        return '巨头'


def fetch_industry_board_info(industry_code, industry_name, stock_codes):
    """
    获取单个行业板块的成分股信息
    
    Args:
        industry_code (str): 行业板块代码
        industry_name (str): 行业板块名称
        stock_codes (list): 股票代码列表
        
    Returns:
        dict: 股票代码到行业的映射
    """
    stock_industry_map = {}
    try:
        # 获取行业成分股
        industry_stocks = ak.stock_board_industry_cons_em(symbol=industry_code)
        # 提取股票代码
        for _, stock_row in industry_stocks.iterrows():
            stock_code = stock_row['代码']
            if stock_code in stock_codes:
                stock_industry_map[stock_code] = industry_name
    except Exception as e:
        logger.warning(f"获取行业 {industry_name} 成分股失败: {str(e)}")
    
    return stock_industry_map


def fetch_industry_info(stock_codes):
    """
    并行获取股票所属行业信息
    
    Args:
        stock_codes (list): 股票代码列表
        
    Returns:
        dict: 股票代码到行业的映射
    """
    try:
        logger.info("开始获取行业板块数据")
        # 获取行业板块列表
        industry_boards = ak.stock_board_industry_name_em()
        
        # 初始化结果字典
        stock_industry_map = {}
        
        # 使用线程池并行获取行业成分股
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # 创建任务列表
            future_to_industry = {}
            for _, row in industry_boards.iterrows():
                industry_name = row['板块名称']
                industry_code = row['板块代码']
                future = executor.submit(fetch_industry_board_info, industry_code, industry_name, stock_codes)
                future_to_industry[future] = industry_name
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_industry):
                industry_name = future_to_industry[future]
                try:
                    result = future.result()
                    stock_industry_map.update(result)
                except Exception as e:
                    logger.warning(f"处理行业 {industry_name} 数据失败: {str(e)}")
        
        logger.info(f"成功获取行业信息，共 {len(stock_industry_map)} 条记录")
        return stock_industry_map
    except Exception as e:
        logger.error(f"获取行业信息失败: {str(e)}")
        return {}


def fetch_concept_board_info(concept_code, concept_name, stock_codes):
    """
    获取单个概念板块的成分股信息
    
    Args:
        concept_code (str): 概念板块代码
        concept_name (str): 概念板块名称
        stock_codes (list): 股票代码列表
        
    Returns:
        dict: 股票代码到概念的映射
    """
    concept_result = {}
    try:
        # 获取概念成分股
        concept_stocks = ak.stock_board_concept_cons_em(symbol=concept_code)
        # 提取股票代码
        for _, stock_row in concept_stocks.iterrows():
            stock_code = stock_row['代码']
            if stock_code in stock_codes:
                if stock_code not in concept_result:
                    concept_result[stock_code] = []
                concept_result[stock_code].append(concept_name)
    except Exception as e:
        logger.warning(f"获取概念 {concept_name} 成分股失败: {str(e)}")
    
    return concept_result


def fetch_concept_info(stock_codes):
    """
    并行获取股票所属概念板块信息（前50个概念）
    
    Args:
        stock_codes (list): 股票代码列表
        
    Returns:
        dict: 股票代码到概念板块列表的映射
    """
    try:
        logger.info("开始获取概念板块数据")
        # 获取概念板块列表
        concept_boards = ak.stock_board_concept_name_em()
        # 只取前50个概念
        concept_boards = concept_boards.head(100)
        
        # 初始化结果字典
        stock_concept_map = {code: [] for code in stock_codes}
        
        # 使用线程池并行获取概念成分股
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # 创建任务列表
            future_to_concept = {}
            for _, row in concept_boards.iterrows():
                concept_name = row['板块名称']
                concept_code = row['板块代码']
                future = executor.submit(fetch_concept_board_info, concept_code, concept_name, stock_codes)
                future_to_concept[future] = concept_name
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_concept):
                concept_name = future_to_concept[future]
                try:
                    result = future.result()
                    # 合并结果
                    for stock_code, concepts in result.items():
                        stock_concept_map[stock_code].extend(concepts)
                except Exception as e:
                    logger.warning(f"处理概念 {concept_name} 数据失败: {str(e)}")
        
        # 将概念列表转换为字符串，用逗号分隔
        for code in stock_concept_map:
            stock_concept_map[code] = ','.join(stock_concept_map[code])
        
        logger.info(f"成功获取概念信息")
        return stock_concept_map
    except Exception as e:
        logger.error(f"获取概念信息失败: {str(e)}")
        return {code: '' for code in stock_codes}


def save_stock_basic():
    """
    获取并保存股票基础数据到数据库
    
    数据表: 基础
    主键: ['代码']
    列顺序: 代码、简称、市场、行业板块、概念板块、总市值、流通市值、市值等级、60日涨跌幅
    
    注意：新表结构中，基础表直接使用中文表名，不再拼接频率
    """
    try:
        # 获取A股全部股票基础信息
        df = fetch_all_stocks()
        
        # 获取股票代码列表
        stock_codes = df['代码'].tolist()
        
        # 并行获取行业和概念信息
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # 提交任务
            industry_future = executor.submit(fetch_industry_info, stock_codes)
            concept_future = executor.submit(fetch_concept_info, stock_codes)
            
            # 添加市场类型和市值等级（这些操作可以在等待API结果时进行）
            df['市场'] = df['代码'].apply(determine_market)
            df['市值等级'] = df['流通市值'].apply(determine_size_category)
            
            # 获取结果
            industry_map = industry_future.result()
            concept_map = concept_future.result()
        
        # 添加行业和概念信息
        df['行业板块'] = df['代码'].map(industry_map).fillna('')
        df['概念板块'] = df['代码'].map(concept_map).fillna('')
        
        # 重命名列
        df = df.rename(columns={'名称': '简称'})
        
        # 调整列顺序
        df = df[['代码', '简称', '市场', '行业板块', '概念板块', '总市值', '流通市值', '市值等级', '60日涨跌幅']]
        
        # 保存数据（完全重写）
        try:
            # 先尝试删除旧表，确保完全重写
            conn = storage._get_connection()
            try:
                table_name = storage._generate_table_name("基础")
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()
                logger.info(f"已删除旧表 {table_name}，准备重建")
            except Exception as e:
                conn.rollback()
                logger.warning(f"删除旧表失败: {str(e)}，将尝试直接保存")
            finally:
                storage._return_connection(conn)
                
            # 保存新数据
            storage.save(
                df=df,
                data_type="基础",
                primary_keys=["代码"],
                replace=True  # 确保完全重写数据表
            )
        except Exception as e:
            logger.error(f"保存数据失败: {str(e)}")
            raise
        logger.info("股票基础数据保存成功")
    except Exception as e:
        logger.error(f"股票基础数据保存失败: {str(e)}")
        raise


def main():
    """
    主函数，获取并保存股票基础数据
    """
    try:
        save_stock_basic()
        logger.info("股票基础数据获取并保存成功")
        return True
    except Exception as e:
        logger.error(f"股票基础数据获取或保存过程中出错: {str(e)}")
        return False


if __name__ == "__main__":
    # 设置日志级别
    logging.basicConfig(level=logging.INFO)
    # 执行主函数
    main()