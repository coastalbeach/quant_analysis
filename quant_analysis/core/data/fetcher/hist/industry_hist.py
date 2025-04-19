#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
行业历史行情数据获取模块

功能：
1. 获取行业板块的历史行情数据
2. 获取行业历史资金流数据
3. 使用akshare的stock_board_industry_hist_em和stock_sector_fund_flow_hist接口获取数据
4. 支持日线、周线、月线三种频率数据获取
5. 将数据存入数据库
"""

import logging
import pandas as pd
import akshare as ak
import datetime
import concurrent.futures
from psycopg import sql
from core.storage import storage
import time
from tqdm import tqdm

# 初始化日志
logger = logging.getLogger('quant.fetcher.hist.industry_hist')

# 设置日志级别，减少不必要的输出
logger.setLevel(logging.INFO)

# 设置常量
START_DATE = "20050104"  # 数据获取的起始日期
END_DATE = datetime.datetime.now().strftime("%Y%m%d")  # 当前日期作为结束日期

# 频率映射
PERIOD_TYPE_MAP = {
    "daily": "日k",
    "weekly": "周k",
    "monthly": "月k"
}

# 复权类型映射
ADJUST_TYPE_MAP = {
    "": "不复权",
    "qfq": "前复权",
    "hfq": "后复权"
}


def fetch_industry_board_names():
    """
    获取行业板块名称列表
    
    Returns:
        list: 行业板块名称列表
    """
    try:
        # 调用akshare接口获取行业板块名称
        df = ak.stock_board_industry_name_em()
        if df.empty:
            logger.warning("获取行业板块名称列表为空")
            return []
        
        # 返回行业板块名称列表
        return df['板块名称'].tolist()
    except Exception as e:
        logger.error(f"获取行业板块名称列表失败: {str(e)}")
        return []


def fetch_industry_hist_data(industry_name, period="daily", adjust="", start_date=START_DATE, end_date=END_DATE):
    """
    获取单个行业板块的历史行情数据
    
    Args:
        industry_name (str): 行业板块名称
        period (str): 数据频率，可选值："daily"(日k)、"weekly"(周k)、"monthly"(月k)
        adjust (str): 复权方式，可选值：""(不复权)、"qfq"(前复权)、"hfq"(后复权)
        start_date (str): 开始日期，默认为全局设置的START_DATE
        end_date (str): 结束日期，默认为全局设置的END_DATE
        
    Returns:
        pandas.DataFrame: 行业板块历史行情数据
    """
    try:
        # 将period映射为接口需要的格式
        period_param = PERIOD_TYPE_MAP.get(period, "日k")
        
        # 调用akshare接口获取历史行情数据
        df = ak.stock_board_industry_hist_em(
            symbol=industry_name,
            start_date=start_date,
            end_date=end_date,
            period=period_param,
            adjust=adjust
        )
        
        if df.empty:
            logger.warning(f"获取行业板块 {industry_name} 的{period_param}历史行情数据为空")
            return pd.DataFrame()
        
        # 确保日期列为日期类型
        df['日期'] = pd.to_datetime(df['日期'])
        
        # 添加频率字段
        df['频率'] = period_param
        
        # 添加复权类型字段
        df['复权类型'] = ADJUST_TYPE_MAP.get(adjust, "不复权")
        
        # 添加行业板块名称字段
        df['行业名称'] = industry_name
        
        return df
    except Exception as e:
        # 记录错误日志
        logger.error(f"获取行业板块 {industry_name} 的{PERIOD_TYPE_MAP.get(period, '日k')}历史行情数据失败: {str(e)}")
        return pd.DataFrame()


def fetch_industry_fund_flow_hist(industry_name):
    """
    获取单个行业的历史资金流数据
    
    Args:
        industry_name (str): 行业名称
        
    Returns:
        pandas.DataFrame: 行业历史资金流数据
    """
    try:
        # 调用akshare接口获取历史资金流数据
        df = ak.stock_sector_fund_flow_hist(symbol=industry_name)
        
        if df.empty:
            logger.warning(f"获取行业 {industry_name} 的历史资金流数据为空")
            return pd.DataFrame()
        
        # 确保日期列为日期类型
        df['日期'] = pd.to_datetime(df['日期'])
        
        # 添加行业名称字段
        df['行业名称'] = industry_name
        
        return df
    except Exception as e:
        # 记录错误日志
        logger.error(f"获取行业 {industry_name} 的历史资金流数据失败: {str(e)}")
        return pd.DataFrame()


def save_industry_hist_data_to_db(df, table_name="行业板块行情"):
    """
    将行业板块历史行情数据保存到数据库
    
    Args:
        df (pandas.DataFrame): 行业板块历史行情数据
        table_name (str): 表名
        
    Returns:
        bool: 是否成功
    """
    if df.empty:
        logger.warning("没有行业板块历史行情数据需要保存")
        return False
    
    try:
        # 使用storage模块保存数据
        conn = storage._get_connection()
        try:
            # 检查表是否存在，不存在则创建
            with conn.cursor() as cur:
                # 构建创建表的SQL语句
                create_table_sql = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    "日期" TIMESTAMP,
                    "行业名称" TEXT,
                    "开盘" DOUBLE PRECISION,
                    "收盘" DOUBLE PRECISION,
                    "最高" DOUBLE PRECISION,
                    "最低" DOUBLE PRECISION,
                    "涨跌幅" DOUBLE PRECISION,
                    "涨跌额" DOUBLE PRECISION,
                    "成交量" BIGINT,
                    "成交额" DOUBLE PRECISION,
                    "振幅" DOUBLE PRECISION,
                    "换手率" DOUBLE PRECISION,
                    "频率" TEXT,
                    "复权类型" TEXT,
                    PRIMARY KEY ("日期", "行业名称", "频率", "复权类型")
                )
                """).format(sql.Identifier(table_name))
                cur.execute(create_table_sql)
            
            # 批量插入数据，遇到主键冲突则更新
            with conn.cursor() as cur:
                # 构建插入或更新的SQL语句
                insert_sql = sql.SQL("""
                INSERT INTO {} ("日期", "行业名称", "开盘", "收盘", "最高", "最低", 
                               "涨跌幅", "涨跌额", "成交量", "成交额", "振幅", "换手率", "频率", "复权类型")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("日期", "行业名称", "频率", "复权类型") DO UPDATE SET
                    "开盘" = EXCLUDED."开盘",
                    "收盘" = EXCLUDED."收盘",
                    "最高" = EXCLUDED."最高",
                    "最低" = EXCLUDED."最低",
                    "涨跌幅" = EXCLUDED."涨跌幅",
                    "涨跌额" = EXCLUDED."涨跌额",
                    "成交量" = EXCLUDED."成交量",
                    "成交额" = EXCLUDED."成交额",
                    "振幅" = EXCLUDED."振幅",
                    "换手率" = EXCLUDED."换手率"
                """).format(sql.Identifier(table_name))
                
                # 准备批量插入的数据
                data_to_insert = []
                for _, row in df.iterrows():
                    data_to_insert.append((
                        row['日期'],
                        row['行业名称'],
                        row['开盘'],
                        row['收盘'],
                        row['最高'],
                        row['最低'],
                        row['涨跌幅'],
                        row['涨跌额'],
                        row['成交量'],
                        row['成交额'],
                        row['振幅'],
                        row['换手率'],
                        row['频率'],
                        row['复权类型']
                    ))
                
                # 使用executemany批量插入
                cur.executemany(insert_sql, data_to_insert)
            
            # 提交事务
            conn.commit()
            logger.info(f"成功保存 {len(df)} 条行业板块历史行情数据到 {table_name} 表")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"保存行业板块历史行情数据到数据库失败: {str(e)}")
            return False
        finally:
            storage._return_connection(conn)
    except Exception as e:
        logger.error(f"获取数据库连接失败: {str(e)}")
        return False


def save_industry_fund_flow_to_db(df, table_name="行业资金流"):
    """
    将行业历史资金流数据保存到数据库
    
    Args:
        df (pandas.DataFrame): 行业历史资金流数据
        table_name (str): 表名
        
    Returns:
        bool: 是否成功
    """
    if df.empty:
        logger.warning("没有行业历史资金流数据需要保存")
        return False
    
    try:
        # 使用storage模块保存数据
        conn = storage._get_connection()
        try:
            # 检查表是否存在，不存在则创建
            with conn.cursor() as cur:
                # 构建创建表的SQL语句
                create_table_sql = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    "日期" TIMESTAMP,
                    "行业名称" TEXT,
                    "主力净流入-净额" DOUBLE PRECISION,
                    "主力净流入-净占比" DOUBLE PRECISION,
                    "超大单净流入-净额" DOUBLE PRECISION,
                    "超大单净流入-净占比" DOUBLE PRECISION,
                    "大单净流入-净额" DOUBLE PRECISION,
                    "大单净流入-净占比" DOUBLE PRECISION,
                    "中单净流入-净额" DOUBLE PRECISION,
                    "中单净流入-净占比" DOUBLE PRECISION,
                    "小单净流入-净额" DOUBLE PRECISION,
                    "小单净流入-净占比" DOUBLE PRECISION,
                    PRIMARY KEY ("日期", "行业名称")
                )
                """).format(sql.Identifier(table_name))
                cur.execute(create_table_sql)
            
            # 批量插入数据，遇到主键冲突则更新
            with conn.cursor() as cur:
                # 构建插入或更新的SQL语句
                insert_sql = sql.SQL("""
                INSERT INTO {} ("日期", "行业名称", "主力净流入-净额", "主力净流入-净占比", 
                               "超大单净流入-净额", "超大单净流入-净占比", "大单净流入-净额", "大单净流入-净占比", 
                               "中单净流入-净额", "中单净流入-净占比", "小单净流入-净额", "小单净流入-净占比")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("日期", "行业名称") DO UPDATE SET
                    "主力净流入-净额" = EXCLUDED."主力净流入-净额",
                    "主力净流入-净占比" = EXCLUDED."主力净流入-净占比",
                    "超大单净流入-净额" = EXCLUDED."超大单净流入-净额",
                    "超大单净流入-净占比" = EXCLUDED."超大单净流入-净占比",
                    "大单净流入-净额" = EXCLUDED."大单净流入-净额",
                    "大单净流入-净占比" = EXCLUDED."大单净流入-净占比",
                    "中单净流入-净额" = EXCLUDED."中单净流入-净额",
                    "中单净流入-净占比" = EXCLUDED."中单净流入-净占比",
                    "小单净流入-净额" = EXCLUDED."小单净流入-净额",
                    "小单净流入-净占比" = EXCLUDED."小单净流入-净占比"
                """).format(sql.Identifier(table_name))
                
                # 准备批量插入的数据
                data_to_insert = []
                for _, row in df.iterrows():
                    data_to_insert.append((
                        row['日期'],
                        row['行业名称'],
                        row['主力净流入-净额'],
                        row['主力净流入-净占比'],
                        row['超大单净流入-净额'],
                        row['超大单净流入-净占比'],
                        row['大单净流入-净额'],
                        row['大单净流入-净占比'],
                        row['中单净流入-净额'],
                        row['中单净流入-净占比'],
                        row['小单净流入-净额'],
                        row['小单净流入-净占比']
                    ))
                
                # 使用executemany批量插入
                cur.executemany(insert_sql, data_to_insert)
            
            # 提交事务
            conn.commit()
            logger.info(f"成功保存 {len(df)} 条行业历史资金流数据到 {table_name} 表")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"保存行业历史资金流数据到数据库失败: {str(e)}")
            return False
        finally:
            storage._return_connection(conn)
    except Exception as e:
        logger.error(f"获取数据库连接失败: {str(e)}")
        return False


def fetch_all_industry_hist(period="daily", adjust="", max_workers=10):
    """
    获取所有行业板块的历史行情数据
    
    Args:
        period (str): 数据频率，可选值："daily"(日k)、"weekly"(周k)、"monthly"(月k)
        adjust (str): 复权方式，可选值：""(不复权)、"qfq"(前复权)、"hfq"(后复权)
        max_workers (int): 最大并行工作线程数
        
    Returns:
        bool: 是否成功
    """
    # 获取所有行业板块名称
    industry_names = fetch_industry_board_names()
    if not industry_names:
        logger.error("未获取到行业板块名称列表，无法继续获取历史行情数据")
        return False
    
    # 使用线程池并行获取数据
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 创建任务
        future_to_industry = {}
        for industry_name in industry_names:
            future = executor.submit(
                fetch_industry_hist_data,
                industry_name,
                period=period,
                adjust=adjust
            )
            future_to_industry[future] = industry_name
        
        # 收集结果
        all_data = []
        for future in tqdm(concurrent.futures.as_completed(future_to_industry), total=len(future_to_industry), desc=f"获取{PERIOD_TYPE_MAP.get(period, '日k')}行业板块数据"):
            industry_name = future_to_industry[future]
            try:
                df = future.result()
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                logger.error(f"获取行业板块 {industry_name} 数据失败: {str(e)}")
    
    # 合并所有数据
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        # 保存到数据库
        success = save_industry_hist_data_to_db(combined_data)
        return success
    else:
        logger.warning("未获取到任何行业板块历史行情数据")
        return False


def fetch_all_industry_fund_flow(max_workers=10):
    """
    获取所有行业的历史资金流数据
    
    Args:
        max_workers (int): 最大并行工作线程数
        
    Returns:
        bool: 是否成功
    """
    # 获取所有行业板块名称
    industry_names = fetch_industry_board_names()
    if not industry_names:
        logger.error("未获取到行业板块名称列表，无法继续获取历史资金流数据")
        return False
    
    # 使用线程池并行获取数据
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 创建任务
        future_to_industry = {}
        for industry_name in industry_names:
            future = executor.submit(
                fetch_industry_fund_flow_hist,
                industry_name
            )
            future_to_industry[future] = industry_name
        
        # 收集结果
        all_data = []
        for future in tqdm(concurrent.futures.as_completed(future_to_industry), total=len(future_to_industry), desc="获取行业历史资金流数据"):
            industry_name = future_to_industry[future]
            try:
                df = future.result()
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                logger.error(f"获取行业 {industry_name} 历史资金流数据失败: {str(e)}")
    
    # 合并所有数据
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        # 保存到数据库
        success = save_industry_fund_flow_to_db(combined_data)
        return success
    else:
        logger.warning("未获取到任何行业历史资金流数据")
        return False


def main():
    """
    主函数
    """
    logger.info("开始获取行业板块历史数据")
    
    # 获取所有频率的行业板块历史行情数据
    for period in ["daily", "weekly", "monthly"]:
        success = fetch_all_industry_hist(period=period, adjust="")
        if success:
            logger.info(f"成功获取行业板块的{PERIOD_TYPE_MAP.get(period, '日k')}历史行情数据")
        else:
            logger.error(f"获取行业板块的{PERIOD_TYPE_MAP.get(period, '日k')}历史行情数据失败")
    
    # 获取行业历史资金流数据
    success = fetch_all_industry_fund_flow()
    if success:
        logger.info("成功获取行业历史资金流数据")
    else:
        logger.error("获取行业历史资金流数据失败")
    
    logger.info("行业板块历史数据获取完成")


if __name__ == "__main__":
    main()