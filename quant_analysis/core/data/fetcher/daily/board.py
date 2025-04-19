#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
行业板块和概念板块数据获取模块

功能：
1. 获取东方财富行业板块数据
2. 获取东方财富概念板块数据
3. 将数据保存到数据库
"""

import logging
import akshare as ak
from core.storage import storage

# 初始化日志
logger = logging.getLogger('quant.fetcher.daily.board')


def fetch_industry_board():
    """
    获取东方财富行业板块数据
    
    接口: stock_board_industry_name_em
    数据源: 东方财富网-沪深京板块-行业板块
    
    Returns:
        pandas.DataFrame: 行业板块数据
    """
    try:
        logger.info("开始获取行业板块数据")
        df = ak.stock_board_industry_name_em()
        logger.info(f"成功获取行业板块数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取行业板块数据失败: {str(e)}")
        raise


def fetch_concept_board():
    """
    获取东方财富概念板块数据
    
    接口: stock_board_concept_name_em
    数据源: 东方财富网-行情中心-沪深京板块-概念板块
    
    Returns:
        pandas.DataFrame: 概念板块数据
    """
    try:
        logger.info("开始获取概念板块数据")
        df = ak.stock_board_concept_name_em()
        logger.info(f"成功获取概念板块数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"获取概念板块数据失败: {str(e)}")
        raise


def save_industry_board():
    """
    获取并保存行业板块数据到数据库
    
    数据表: 行业板块
    主键: ['板块代码']
    """
    try:
        # 获取数据
        df = fetch_industry_board()
        
        # 保存数据
        storage.save(
            df=df,
            data_type="行业板块",
            primary_keys=["板块代码"],
            replace=False
        )
        logger.info("行业板块数据保存成功")
    except Exception as e:
        logger.error(f"行业板块数据保存失败: {str(e)}")
        raise


def save_concept_board():
    """
    获取并保存概念板块数据到数据库
    
    数据表: 概念板块
    主键: ['板块代码']
    """
    try:
        # 获取数据
        df = fetch_concept_board()
        
        # 保存数据
        storage.save(
            df=df,
            data_type="概念板块",
            primary_keys=["板块代码"],
            replace=False
        )
        logger.info("概念板块数据保存成功")
    except Exception as e:
        logger.error(f"概念板块数据保存失败: {str(e)}")
        raise


def main():
    """
    主函数，获取并保存所有板块数据
    """
    try:
        # 获取并保存行业板块数据
        save_industry_board()
        
        # 获取并保存概念板块数据
        save_concept_board()
        
        logger.info("所有板块数据获取并保存成功")
        return True
    except Exception as e:
        logger.error(f"板块数据获取或保存过程中出错: {str(e)}")
        return False


if __name__ == "__main__":
    # 设置日志级别
    logging.basicConfig(level=logging.INFO)
    # 执行主函数
    main()