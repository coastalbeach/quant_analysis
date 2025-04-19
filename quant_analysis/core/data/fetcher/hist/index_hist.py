#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
指数历史行情数据获取模块

功能：
1. 获取主要指数的历史行情数据
2. 使用akshare的index_zh_a_hist接口获取指数历史数据
3. 支持日线、周线、月线三种频率数据获取
4. 将数据存入数据库
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
logger = logging.getLogger('quant.fetcher.hist.index_hist')

# 设置日志级别，减少不必要的输出
logger.setLevel(logging.INFO)

# 设置常量
START_DATE = "20050104"  # 数据获取的起始日期
END_DATE = datetime.datetime.now().strftime("%Y%m%d")  # 当前日期作为结束日期
PERIOD_TYPES = ["daily", "weekly", "monthly"]  # 日线、周线、月线

# 频率映射
PERIOD_TYPE_MAP = {
    "daily": "日线",
    "weekly": "周线",
    "monthly": "月线"
}

# 主要指数代码和名称映射
INDEX_MAP = {
    "000001": "上证指数",
    "399001": "深证成指",
    "399006": "创业板指",
    "000688": "科创50",
    "000016": "上证50",
    "000985": "中证全指",
    "000300": "沪深300",
    "000852": "中证1000",
    "000010": "上证180",
    "000905": "中证500",
    "000922": "中证红利",
    "000978": "医药100",
    "399550": "央视50",
    "000015": "红利指数",
    "000991": "全指医药",
    "000827": "中证环保",
    "000931": "中证可选",
    "000932": "中证消费",
    "000933": "中证医药",
    "000934": "中证金融",
    "000935": "中证信息",
    "000036": "上证分级",
    "000037": "上证医药",
    "000038": "上证金融",
    "000039": "上证信息",
    "000040": "上证可选",
    "000041": "上证消费",
    "000042": "上证综指",
    "000043": "上证超大",
    "000044": "上证中盘",
    "000045": "上证小盘",
    "000046": "上证中小",
    "000047": "上证全指",
    "000155": "市值百强",
    "399673": "创业板50",
    "399330": "深证100",
    "399311": "国证1000",
    "399324": "深证红利",
    "399551": "央视创新",
    "399989": "中证医疗",
    "399971": "中证传媒",
    "399975": "证券公司",
    "399986": "中证银行",
    "399440": "国证钢铁",
    "399395": "国证有色",
    "399998": "中证煤炭",
    "399997": "中证白酒",
    "399996": "中证食品",
    "399441": "国证油气",
    "399394": "国证医药",
    "399618": "深证医药",
    "399365": "国证农业",
    "399364": "国证文化",
    "399363": "国证传媒",
    "399362": "国证民营",
    "399367": "国证大宗",
    "399368": "国证军工",
    "399373": "国证地产",
    "399374": "国证酒店",
    "399375": "国证有色",
    "399376": "国证煤炭",
    "399377": "国证环保",
    "399378": "国证电力",
    "399379": "国证基金",
    "399380": "国证金融",
    "399381": "国证券商",
    "399382": "国证医药",
    "399383": "国证食品",
    "399384": "国证材料",
    "399385": "国证服务",
    "399386": "国证消费",
    "399387": "国证电子",
    "399388": "国证通信",
    "399389": "国证计算",
    "399390": "国证制造",
    "399391": "国证汽车",
    "399392": "国证建材",
    "399393": "国证家电",
    "399950": "300基建",
    "399951": "300银行",
    "399952": "300地产",
    "399953": "300医药",
    "399954": "300材料",
    "399955": "300工业",
    "399956": "300消费",
    "399957": "300金融",
    "399958": "300信息",
    "399959": "300龙头",
    "399960": "300价值",
    "399961": "300成长",
    "399962": "300低波",
    "399963": "300高贝",
    "399964": "300民企",
    "399965": "800材料",
    "399966": "800消费",
    "399967": "800金融",
    "399968": "300周期",
    "399969": "300非周",
    "399970": "800周期",
    "399971": "800非周",
    "399972": "300深市",
    "399973": "中证50A",
    "399974": "中证50B",
    "399975": "证券公司",
    "399976": "CS新能",
    "399982": "500等权",
    "399983": "地产等权",
    "399984": "300等权",
    "399985": "中证全指",
    "399986": "中证银行",
    "399987": "中证酒",
    "399988": "中证医疗",
    "399989": "中证医药",
    "399990": "煤炭等权",
    "399991": "一带一路",
    "399992": "CSWD并购",
    "399993": "CSWD生科",
    "399994": "信息安全",
    "399995": "基建工程",
    "399996": "智能家居",
    "399997": "中证白酒",
    "399998": "中证煤炭"
}


def fetch_index_hist_data(index_code, period="daily", start_date=START_DATE, end_date=END_DATE):
    """
    获取单个指数的历史行情数据
    
    Args:
        index_code (str): 指数代码
        period (str): 数据频率，可选值："daily"(日线)、"weekly"(周线)、"monthly"(月线)
        start_date (str): 开始日期，默认为全局设置的START_DATE
        end_date (str): 结束日期，默认为全局设置的END_DATE
        
    Returns:
        pandas.DataFrame: 指数历史行情数据
    """
    try:
        # 调用akshare接口获取历史行情数据
        df = ak.index_zh_a_hist(
            symbol=index_code,
            period=period,
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty:
            logger.warning(f"获取指数 {index_code} 的{PERIOD_TYPE_MAP.get(period, '日线')}历史行情数据为空")
            return pd.DataFrame()
        
        # 确保日期列为日期类型
        df['日期'] = pd.to_datetime(df['日期'])
        
        # 添加频率字段
        df['频率'] = PERIOD_TYPE_MAP.get(period, "日线")
        
        # 添加指数代码和名称字段
        df['指数代码'] = index_code
        df['指数名称'] = INDEX_MAP.get(index_code, "未知指数")
        
        return df
    except Exception as e:
        # 记录错误日志
        logger.error(f"获取指数 {index_code} 的{PERIOD_TYPE_MAP.get(period, '日线')}历史行情数据失败: {str(e)}")
        return pd.DataFrame()


def save_index_data_to_db(df, table_name="指数行情"):
    """
    将指数数据保存到数据库
    
    Args:
        df (pandas.DataFrame): 指数数据
        table_name (str): 表名
        
    Returns:
        bool: 是否成功
    """
    if df.empty:
        logger.warning("没有数据需要保存")
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
                    "指数代码" TEXT,
                    "指数名称" TEXT,
                    "开盘" DOUBLE PRECISION,
                    "收盘" DOUBLE PRECISION,
                    "最高" DOUBLE PRECISION,
                    "最低" DOUBLE PRECISION,
                    "成交量" BIGINT,
                    "成交额" DOUBLE PRECISION,
                    "振幅" DOUBLE PRECISION,
                    "涨跌幅" DOUBLE PRECISION,
                    "涨跌额" DOUBLE PRECISION,
                    "换手率" DOUBLE PRECISION,
                    "频率" TEXT,
                    PRIMARY KEY ("日期", "指数代码", "频率")
                )
                """).format(sql.Identifier(table_name))
                cur.execute(create_table_sql)
            
            # 批量插入数据，遇到主键冲突则更新
            with conn.cursor() as cur:
                # 构建插入或更新的SQL语句
                insert_sql = sql.SQL("""
                INSERT INTO {} ("日期", "指数代码", "指数名称", "开盘", "收盘", "最高", "最低", 
                               "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率", "频率")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ("日期", "指数代码", "频率") DO UPDATE SET
                    "指数名称" = EXCLUDED."指数名称",
                    "开盘" = EXCLUDED."开盘",
                    "收盘" = EXCLUDED."收盘",
                    "最高" = EXCLUDED."最高",
                    "最低" = EXCLUDED."最低",
                    "成交量" = EXCLUDED."成交量",
                    "成交额" = EXCLUDED."成交额",
                    "振幅" = EXCLUDED."振幅",
                    "涨跌幅" = EXCLUDED."涨跌幅",
                    "涨跌额" = EXCLUDED."涨跌额",
                    "换手率" = EXCLUDED."换手率"
                """).format(sql.Identifier(table_name))
                
                # 准备批量插入的数据
                data_to_insert = []
                for _, row in df.iterrows():
                    data_to_insert.append((
                        row['日期'],
                        row['指数代码'],
                        row['指数名称'],
                        row['开盘'],
                        row['收盘'],
                        row['最高'],
                        row['最低'],
                        row['成交量'],
                        row['成交额'],
                        row['振幅'],
                        row['涨跌幅'],
                        row['涨跌额'],
                        row['换手率'],
                        row['频率']
                    ))
                
                # 使用executemany批量插入
                cur.executemany(insert_sql, data_to_insert)
            
            # 提交事务
            conn.commit()
            logger.info(f"成功保存 {len(df)} 条指数数据到 {table_name} 表")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"保存指数数据到数据库失败: {str(e)}")
            return False
        finally:
            storage._return_connection(conn)
    except Exception as e:
        logger.error(f"获取数据库连接失败: {str(e)}")
        return False


def fetch_all_indices(period="daily", max_workers=10):
    """
    获取所有指数的历史行情数据
    
    Args:
        period (str): 数据频率，可选值："daily"(日线)、"weekly"(周线)、"monthly"(月线)
        max_workers (int): 最大并行工作线程数
        
    Returns:
        bool: 是否成功
    """
    # 获取所有指数代码
    index_codes = list(INDEX_MAP.keys())
    
    # 使用线程池并行获取数据
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 创建任务
        future_to_index = {}
        for index_code in index_codes:
            future = executor.submit(
                fetch_index_hist_data,
                index_code,
                period=period
            )
            future_to_index[future] = index_code
        
        # 收集结果
        all_data = []
        for future in tqdm(concurrent.futures.as_completed(future_to_index), total=len(future_to_index), desc=f"获取{PERIOD_TYPE_MAP.get(period, '日线')}指数数据"):
            index_code = future_to_index[future]
            try:
                df = future.result()
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                logger.error(f"获取指数 {index_code} 数据失败: {str(e)}")
    
    # 合并所有数据
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        # 保存到数据库
        success = save_index_data_to_db(combined_data)
        return success
    else:
        logger.warning("未获取到任何指数数据")
        return False


def fetch_main_indices(period="daily"):
    """
    获取主要指数的历史行情数据
    
    Args:
        period (str): 数据频率，可选值："daily"(日线)、"weekly"(周线)、"monthly"(月线)
        
    Returns:
        bool: 是否成功
    """
    # 主要指数代码
    main_indices = [
        "000001",  # 上证指数
        "399001",  # 深证成指
        "399006",  # 创业板指
        "000688",  # 科创50
        "000016",  # 上证50
        "000985",  # 中证全指
        "000300",  # 沪深300
        "000852",  # 中证1000
        "000905"   # 中证500
    ]
    
    # 收集所有数据
    all_data = []
    for index_code in tqdm(main_indices, desc=f"获取主要{PERIOD_TYPE_MAP.get(period, '日线')}指数数据"):
        try:
            df = fetch_index_hist_data(index_code, period=period)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"获取指数 {index_code} 数据失败: {str(e)}")
    
    # 合并所有数据
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        # 保存到数据库
        success = save_index_data_to_db(combined_data)
        return success
    else:
        logger.warning("未获取到任何主要指数数据")
        return False


def main():
    """
    主函数
    """
    logger.info("开始获取指数历史行情数据")
    
    # 获取所有频率的主要指数数据
    for period in PERIOD_TYPES:
        success = fetch_main_indices(period=period)
        if success:
            logger.info(f"成功获取主要指数的{PERIOD_TYPE_MAP.get(period, '日线')}历史行情数据")
        else:
            logger.error(f"获取主要指数的{PERIOD_TYPE_MAP.get(period, '日线')}历史行情数据失败")
    
    logger.info("指数历史行情数据获取完成")


if __name__ == "__main__":
    main()