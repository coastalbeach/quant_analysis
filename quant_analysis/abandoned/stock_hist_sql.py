#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票历史行情数据获取模块 (SQL优化版)

功能：
1. 使用PostgreSQL的COPY命令进行高效批量数据导入
2. 利用PostgreSQL的窗口函数和聚合功能直接在数据库中生成周线和月线数据
3. 使用数据库事务和批处理减少网络往返
4. 优化并行处理策略，平衡CPU和IO资源
5. 实现数据缓存和增量更新机制
"""

import logging
import pandas as pd
import numpy as np
import akshare as ak
import datetime
import concurrent.futures
import io
import time
import os
import tempfile
import psycopg
from psycopg import sql
from psycopg.errors import UniqueViolation
from core.storage import storage
from tqdm import tqdm

# 初始化日志
logger = logging.getLogger('quant.fetcher.hist.stock_hist_sql')

# 设置日志级别，减少不必要的输出
logger.setLevel(logging.WARNING)  # 默认只输出WARNING及以上级别的日志

# 设置常量
START_DATE = "20050104"  # 数据获取的起始日期
END_DATE = datetime.datetime.now().strftime("%Y%m%d")  # 当前日期作为结束日期
ADJUST_TYPES = ["hfq", "qfq"]  # 后复权、前复权（不复权数据暂不获取）
PERIOD_TYPES = ["daily", "weekly", "monthly"]  # 日线、周线、月线

# 复权类型映射
ADJUST_TYPE_MAP = {
    "": "原始",
    "qfq": "前复权",
    "hfq": "后复权"
}

# 频率映射
PERIOD_TYPE_MAP = {
    "daily": "日线",
    "weekly": "周线",
    "monthly": "月线"
}

# 批处理大小
BATCH_SIZE = 100  # 每批处理的股票数量
COPY_BATCH_SIZE = 10000  # COPY命令批处理大小

# 市场信息缓存
MARKET_INFO_CACHE = {}


def setup_database():
    """
    设置数据库环境，创建必要的函数、索引和触发器
    """
    conn = storage._get_connection()
    try:
        with conn.cursor() as cur:
            # 创建临时表用于COPY操作
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_hist_staging (
                    "股票代码" TEXT,
                    "日期" DATE,
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
                    "复权类型" TEXT,
                    "市场" TEXT
                )
            """)
            
            # 创建生成周线数据的函数
            cur.execute("""
                CREATE OR REPLACE FUNCTION generate_weekly_data(start_date DATE, end_date DATE, adjust_type TEXT)
                RETURNS VOID AS $$
                BEGIN
                    -- 删除现有的临时周线数据
                    DELETE FROM "股票行情_" || adjust_type
                    WHERE "频率" = '周线' AND "日期" BETWEEN start_date AND end_date;
                    
                    -- 插入新的周线数据
                    INSERT INTO "股票行情_" || adjust_type
                    ("股票代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", 
                     "振幅", "涨跌幅", "涨跌额", "换手率", "频率", "复权类型", "市场")
                    SELECT 
                        "股票代码",
                        date_trunc('week', "日期") + interval '4 days' AS "日期", -- 周五
                        first_value("开盘") OVER w AS "开盘",
                        last_value("收盘") OVER w AS "收盘",
                        MAX("最高") OVER w AS "最高",
                        MIN("最低") OVER w AS "最低",
                        SUM("成交量") OVER w AS "成交量",
                        SUM("成交额") OVER w AS "成交额",
                        ROUND(((MAX("最高") OVER w - MIN("最低") OVER w) / first_value("开盘") OVER w * 100)::numeric, 2) AS "振幅",
                        ROUND(((last_value("收盘") OVER w - first_value("开盘") OVER w) / first_value("开盘") OVER w * 100)::numeric, 2) AS "涨跌幅",
                        ROUND((last_value("收盘") OVER w - first_value("开盘") OVER w)::numeric, 2) AS "涨跌额",
                        ROUND(SUM("换手率") OVER w::numeric, 2) AS "换手率",
                        '周线' AS "频率",
                        "复权类型",
                        "市场"
                    FROM "股票行情_" || adjust_type
                    WHERE "频率" = '日线' AND "日期" BETWEEN start_date AND end_date
                    WINDOW w AS (PARTITION BY "股票代码", date_trunc('week', "日期") ORDER BY "日期" 
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            # 创建生成月线数据的函数
            cur.execute("""
                CREATE OR REPLACE FUNCTION generate_monthly_data(start_date DATE, end_date DATE, adjust_type TEXT)
                RETURNS VOID AS $$
                BEGIN
                    -- 删除现有的临时月线数据
                    DELETE FROM "股票行情_" || adjust_type
                    WHERE "频率" = '月线' AND "日期" BETWEEN start_date AND end_date;
                    
                    -- 插入新的月线数据
                    INSERT INTO "股票行情_" || adjust_type
                    ("股票代码", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", 
                     "振幅", "涨跌幅", "涨跌额", "换手率", "频率", "复权类型", "市场")
                    SELECT 
                        "股票代码",
                        date_trunc('month', "日期") + interval '1 month - 1 day' AS "日期", -- 月末
                        first_value("开盘") OVER m AS "开盘",
                        last_value("收盘") OVER m AS "收盘",
                        MAX("最高") OVER m AS "最高",
                        MIN("最低") OVER m AS "最低",
                        SUM("成交量") OVER m AS "成交量",
                        SUM("成交额") OVER m AS "成交额",
                        ROUND(((MAX("最高") OVER m - MIN("最低") OVER m) / first_value("开盘") OVER m * 100)::numeric, 2) AS "振幅",
                        ROUND(((last_value("收盘") OVER m - first_value("开盘") OVER m) / first_value("开盘") OVER m * 100)::numeric, 2) AS "涨跌幅",
                        ROUND((last_value("收盘") OVER m - first_value("开盘") OVER m)::numeric, 2) AS "涨跌额",
                        ROUND(SUM("换手率") OVER m::numeric, 2) AS "换手率",
                        '月线' AS "频率",
                        "复权类型",
                        "市场"
                    FROM "股票行情_" || adjust_type
                    WHERE "频率" = '日线' AND "日期" BETWEEN start_date AND end_date
                    WINDOW m AS (PARTITION BY "股票代码", date_trunc('month', "日期") ORDER BY "日期" 
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            # 创建批量更新函数
            cur.execute("""
                CREATE OR REPLACE FUNCTION batch_upsert_stock_hist()
                RETURNS VOID AS $$
                DECLARE
                    adjust_types TEXT[] := ARRAY['前复权', '后复权'];
                    markets TEXT[] := ARRAY['沪A', '深A', '创', '科创', '京A'];
                    periods TEXT[] := ARRAY['日线', '周线', '月线'];
                    adjust_type TEXT;
                    market TEXT;
                    period TEXT;
                    target_table TEXT;
                BEGIN
                    -- 为每个分区表插入数据
                    FOREACH adjust_type IN ARRAY adjust_types LOOP
                        FOREACH market IN ARRAY markets LOOP
                            FOREACH period IN ARRAY periods LOOP
                                -- 构建目标表名
                                target_table := '股票行情_' || adjust_type || '_' || market || '_' || period;
                                
                                -- 检查表是否存在
                                IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = target_table) THEN
                                    -- 从临时表插入到目标表
                                    EXECUTE 'INSERT INTO "' || target_table || '" 
                                            SELECT s.*, NOW(), NOW() 
                                            FROM stock_hist_staging s 
                                            WHERE s."复权类型" = ''' || adjust_type || ''' 
                                              AND s."市场" = ''' || market || ''' 
                                              AND s."频率" = ''' || period || ''' 
                                            ON CONFLICT ("股票代码", "日期", "市场", "频率") 
                                            DO UPDATE SET 
                                                "开盘" = EXCLUDED."开盘",
                                                "收盘" = EXCLUDED."收盘",
                                                "最高" = EXCLUDED."最高",
                                                "最低" = EXCLUDED."最低",
                                                "成交量" = EXCLUDED."成交量",
                                                "成交额" = EXCLUDED."成交额",
                                                "振幅" = EXCLUDED."振幅",
                                                "涨跌幅" = EXCLUDED."涨跌幅",
                                                "涨跌额" = EXCLUDED."涨跌额",
                                                "换手率" = EXCLUDED."换手率",
                                                updated_at = NOW()';
                                END IF;
                            END LOOP;
                        END LOOP;
                    END LOOP;
                    
                    -- 清空临时表
                    TRUNCATE TABLE stock_hist_staging;
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            # 创建索引优化查询性能
            cur.execute("""
                DO $$
                DECLARE
                    adjust_types TEXT[] := ARRAY['前复权', '后复权'];
                    markets TEXT[] := ARRAY['沪A', '深A', '创', '科创', '京A'];
                    periods TEXT[] := ARRAY['日线', '周线', '月线'];
                    idx_name TEXT;
                    table_name TEXT;
                BEGIN
                    -- 为每个分区表创建索引
                    FOREACH adjust_type IN ARRAY adjust_types LOOP
                        FOREACH market IN ARRAY markets LOOP
                            FOREACH period IN ARRAY periods LOOP
                                table_name := '股票行情_' || adjust_type || '_' || market || '_' || period;
                                
                                -- 检查表是否存在
                                IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = table_name) THEN
                                    -- 创建日期索引
                                    idx_name := table_name || '_date_idx';
                                    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = idx_name) THEN
                                        EXECUTE 'CREATE INDEX IF NOT EXISTS ' || idx_name || ' ON "' || table_name || '" ("日期")';
                                    END IF;
                                    
                                    -- 创建股票代码索引
                                    idx_name := table_name || '_code_idx';
                                    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = idx_name) THEN
                                        EXECUTE 'CREATE INDEX IF NOT EXISTS ' || idx_name || ' ON "' || table_name || '" ("股票代码")';
                                    END IF;
                                END IF;
                            END LOOP;
                        END LOOP;
                    END LOOP;
                END;
                $$;
            """)
            
        conn.commit()
        logger.info("成功设置数据库环境")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"设置数据库环境失败: {str(e)}")
        return False
    finally:
        storage._return_connection(conn)


def get_stock_codes_from_db():
    """
    从基础表获取股票代码和市场信息
    
    Returns:
        tuple: (股票代码列表, 股票代码到市场的映射字典)
    """
    # 检查缓存
    if 'stock_codes' in MARKET_INFO_CACHE and 'market_map' in MARKET_INFO_CACHE:
        return MARKET_INFO_CACHE['stock_codes'], MARKET_INFO_CACHE['market_map']
        
    try:
        logger.info("开始从基础表获取股票代码和市场信息")
        conn = storage._get_connection()
        try:
            # 直接使用SQL查询获取所有股票代码和市场信息
            with conn.cursor() as cur:
                cur.execute("SELECT \"代码\", \"市场\" FROM 基础")
                results = cur.fetchall()
                
                # 处理结果
                stock_codes = [row["代码"] if isinstance(row, dict) else row[0] for row in results]
                market_map = {row["代码"] if isinstance(row, dict) else row[0]: 
                             row["市场"] if isinstance(row, dict) else row[1] for row in results}
            
            # 更新缓存
            MARKET_INFO_CACHE['stock_codes'] = stock_codes
            MARKET_INFO_CACHE['market_map'] = market_map
            
            logger.info(f"成功获取股票代码和市场信息，共 {len(stock_codes)} 只股票")
            return stock_codes, market_map
        except Exception as e:
            logger.error(f"从数据库获取股票代码和市场信息失败: {str(e)}")
            raise
        finally:
            storage._return_connection(conn)
    except Exception as e:
        logger.error(f"获取股票代码和市场信息失败: {str(e)}")
        return [], {}


def get_latest_dates_batch(stock_codes, adjust_name, period_name="日线"):
    """
    批量获取股票在数据库中的最新日期
    
    Args:
        stock_codes (list): 股票代码列表
        adjust_name (str): 复权类型名称（后复权、前复权、原始）
        period_name (str): 频率名称（日线、周线、月线），默认为日线
        
    Returns:
        dict: 股票代码到最新日期的映射字典
    """
    if not stock_codes:
        return {}
        
    latest_dates = {}
    conn = storage._get_connection()
    try:
        # 使用单个高效查询获取所有股票的最新日期
        query = """
            SELECT "股票代码", MAX("日期") as latest_date 
            FROM "股票行情_{}" 
            WHERE "股票代码" = ANY(%s) AND "频率" = %s
            GROUP BY "股票代码"
        """.format(adjust_name)
        
        with conn.cursor() as cur:
            cur.execute(query, [stock_codes, period_name])
            results = cur.fetchall()
            for row in results:
                # 将日期转换为字符串格式YYYYMMDD
                stock_code = row["股票代码"] if isinstance(row, dict) else row[0]
                date_value = row["latest_date"] if isinstance(row, dict) else row[1]
                latest_dates[stock_code] = date_value.strftime("%Y%m%d") if date_value else None
        
        return latest_dates
    except Exception as e:
        logger.error(f"获取最新日期信息失败: {str(e)}")
        return {}
    finally:
        storage._return_connection(conn)


def fetch_stock_hist_data(stock_code, period="daily", adjust="hfq", start_date=START_DATE):
    """
    获取单只股票的历史行情数据
    
    Args:
        stock_code (str): 股票代码
        period (str): 数据频率，可选值："daily"(日线)、"weekly"(周线)、"monthly"(月线)
        adjust (str): 复权方式，可选值：""(不复权)、"qfq"(前复权)、"hfq"(后复权)
        start_date (str): 开始日期，默认为全局设置的START_DATE
        
    Returns:
        pandas.DataFrame: 股票历史行情数据
    """
    try:
        # 调用akshare接口获取历史行情数据
        df = ak.stock_zh_a_hist(
            symbol=stock_code,
            period=period,
            start_date=start_date,
            end_date=END_DATE,
            adjust=adjust
        )
        
        if df.empty:
            return pd.DataFrame()
        
        # 确保日期列为日期类型
        df['日期'] = pd.to_datetime(df['日期'])
        
        # 添加频率字段
        df['频率'] = PERIOD_TYPE_MAP.get(period, "日线")
        
        # 添加复权类型字段
        df['复权类型'] = ADJUST_TYPE_MAP.get(adjust, "后复权")
        
        # 添加股票代码字段（确保存在）
        if '股票代码' not in df.columns:
            df['股票代码'] = stock_code
        
        return df
    except Exception as e:
        # 只记录错误日志，减少日志输出
        logger.error(f"获取股票 {stock_code} 的{PERIOD_TYPE_MAP.get(period, '日线')}历史行情数据失败: {str(e)}")
        return pd.DataFrame()


def bulk_copy_to_staging(data_frames):
    """
    将多个DataFrame批量复制到临时表
    
    Args:
        data_frames (list): DataFrame列表
        
    Returns:
        int: 成功导入的记录数
    """
    if not data_frames:
        return 0
    
    # 合并所有数据
    combined_df = pd.concat(data_frames, ignore_index=True)
    if combined_df.empty:
        return 0
    
    # 确保日期列为字符串格式YYYY-MM-DD
    combined_df['日期'] = pd.to_datetime(combined_df['日期']).dt.strftime('%Y-%m-%d')
    
    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8') as temp_file:
        # 将数据写入CSV格式的临时文件
        combined_df.to_csv(temp_file.name, sep='\t', header=False, index=False, na_rep='\\N')
        temp_file_path = temp_file.name
    
    try:
        conn = storage._get_connection()
        try:
            # 使用COPY命令导入数据到临时表
            with conn.cursor() as cur:
                # 清空临时表
                cur.execute("TRUNCATE TABLE stock_hist_staging")
                
                # 复制数据
                with open(temp_file_path, 'r', encoding='utf-8') as f:
                    columns = ', '.join(f'"{col}"' for col in combined_df.columns)
                    copy_sql = f"""COPY stock_hist_staging ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL '\\N')"""
                    cur.copy_expert(copy_sql, f)
                
                # 调用批量更新函数
                cur.execute("SELECT batch_upsert_stock_hist()")
                
                # 获取导入的记录数
                record_count = len(combined_df)
            
            conn.commit()
            return record_count
        except Exception as e:
            conn.rollback()
            logger.error(f"批量导入数据失败: {str(e)}")
            return 0
        finally:
            storage._return_connection(conn)
    finally:
        # 删除临时文件
        try:
            os.remove(temp_file_path)
        except:
            pass


def process_stock_batch(stock_codes, market_map, period="daily", adjust="hfq", start_date=START_DATE):
    """
    批量处理股票历史数据并保存
    
    Args:
        stock_codes (list): 股票代码列表
        market_map (dict): 股票代码到市场的映射字典
        period (str): 数据频率，可选值："daily"(日线)、"weekly"(周线)、"monthly"(月线)
        adjust (str): 复权方式，可选值：""(不复权)、"qfq"(前复权)、"hfq"(后复权)
        start_date (str): 开始日期，默认为全局设置的START_DATE
        
    Returns:
        int: 成功处理的股票数量
    """
    if not stock_codes:
        return 0
        
    success_count = 0
    error_count = 0
    adjust_name = ADJUST_TYPE_MAP.get(adjust, "后复权")
    period_name = PERIOD_TYPE_MAP.get(period, "日线")
    
    # 批量获取最新日期
    latest_dates = get_latest_dates_batch(stock_codes, adjust_name, period_name)
    
    # 收集所有数据
    all_data = []
    error_stocks = []
    
    # 使用tqdm创建进度条
    for stock_code in tqdm(stock_codes, desc=f"获取{period_name}数据({adjust_name})"):
        try:
            # 确定获取数据的起始日期
            fetch_start_date = start_date
            if stock_code in latest_dates and latest_dates[stock_code]:
                # 使用表内最后日期作为获取数据起始日期
                fetch_start_date = latest_dates[stock_code]
            
            # 获取历史行情数据
            df = fetch_stock_hist_data(stock_code, period, adjust, fetch_start_date)
            if df.empty:
                # 可能是因为没有新数据，也算成功
                if stock_code in latest_dates and latest_dates[stock_code]:
                    success_count += 1
                continue
            
            # 添加市场字段
            if stock_code in market_map:
                df['市场'] = market_map[stock_code]
            
            # 收集数据
            all_data.append(df)
            success_count += 1
            
        except Exception as e:
            error_count += 1
            error_stocks.append((stock_code, str(e)))
    
    # 批量保存到数据库
    if all_data:
        try:
            # 使用批量COPY导入数据
            records_imported = bulk_copy_to_staging(all_data)
            logger.info(f"成功导入 {records_imported} 条{period_name}记录，复权方式: {adjust_name}")
        except Exception as e:
            logger.error(f"批量导入{period_name}数据失败: {str(e)}")
            error_count += len(all_data)
    
    # 批次处理完成后汇总记录日志
    if error_count > 0:
        # 只记录前5个错误的详细信息
        for i, (code, error_msg) in enumerate(error_stocks[:5]):
            logger.error(f"处理股票 {code} 数据失败: {error_msg}")
        
        if len(error_stocks) > 5:
            logger.error(f"... 还有 {len(error_stocks) - 5} 只股票处理失败 ...")
    
    return success_count


def fetch_all_stock_hist_data(period="daily", adjust="hfq", max_workers=10, start_date=START_DATE, force_update=False):
    """
    并行获取所有股票的历史行情数据
    
    Args:
        period (str): 数据频率，可选值："daily"(日线)、"weekly"(周线)、"monthly"(月线)
        adjust (str): 复权方式，可选值：""(不复权)、"qfq"(前复权)、"hfq"(后复权)
        max_workers (int): 最大并行工作线程数
        start_date (str): 开始日期，默认为全局设置的START_DATE
        force_update (bool): 是否强制更新所有数据，忽略数据库中的最新日期
        
    Returns:
        bool: 是否成功
    """
    try:
        # 获取股票代码列表和市场信息
        stock_codes, market_map = get_stock_codes_from_db()
        if not stock_codes:
            logger.error("未获取到股票代码，无法继续获取历史行情数据")
            return False
            
        adjust_name = ADJUST_TYPE_MAP.get(adjust, "后复权")
        period_name = PERIOD_TYPE_MAP.get(period, "日线")
        
        # 保存当前日志级别，并临时提高日志级别以显示重要信息
        log_level_before = logger.level
        logger.setLevel(logging.INFO)
        
        # 如果强制更新，使用指定的起始日期
        if force_update:
            logger.info(f"强制更新模式：从头开始获取{period_name}数据({adjust_name})")
            start_date = START_DATE
    
    except Exception as e:
        logger.error(f"获取股票代码和市场信息失败: {str(e)}")
        return False