#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票历史行情数据获取模块 (优化版)

功能：
1. 从基础表获取股票代码列表
2. 使用akshare的stock_zh_a_hist接口获取2005年1月4日以后的全部历史行情数据
3. 支持日线、周线、月线三种频率数据获取
4. 支持前复权、后复权数据分表存储
5. 使用PostgreSQL COPY命令进行高效批量数据导入
6. 优化数据库连接和事务管理
7. 改进并行处理策略
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
from psycopg import sql
from psycopg.errors import UniqueViolation
from core.storage import storage
from tqdm import tqdm

# 初始化日志
logger = logging.getLogger('quant.fetcher.hist.stock_hist_optimized')

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

# 数据库表结构缓存
TABLE_SCHEMA_CACHE = {}

# 市场信息缓存
MARKET_INFO_CACHE = {}


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
            # 新表结构中，基础表直接使用中文表名，不再拼接频率
            table_name = storage._generate_table_name("基础")
            # 使用psycopg3兼容的方式构建查询
            from psycopg.sql import SQL, Identifier
            query = SQL("SELECT \"代码\", \"市场\" FROM {}").format(Identifier(table_name))
            
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
                # 处理结果时考虑psycopg3可能返回字典的情况
                if results and isinstance(results[0], dict):
                    stock_codes = [row["代码"] for row in results]
                    market_map = {row["代码"]: row["市场"] for row in results}
                else:
                    stock_codes = [row[0] for row in results]
                    market_map = {row[0]: row[1] for row in results}
            
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


def get_latest_dates_from_db_batch(stock_codes, adjust_name, period_name="日线"):
    """
    批量获取股票在数据库中的最新日期（优化版）
    
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
    max_retries = 2
    retry_count = 0
    
    while retry_count < max_retries:
        conn = None
        try:
            conn = storage._get_connection()
            
            # 使用单个查询获取所有股票的最新日期
            table_name = f"股票行情_{adjust_name}"
            query = sql.SQL("""
                SELECT "股票代码", MAX("日期") as latest_date 
                FROM {} 
                WHERE "股票代码" = ANY(%s) AND "频率" = %s
                GROUP BY "股票代码"
            """).format(sql.Identifier(table_name))
            
            with conn.cursor() as cur:
                cur.execute(query, [stock_codes, period_name])
                results = cur.fetchall()
                for row in results:
                    # 将日期转换为字符串格式YYYYMMDD
                    latest_dates[row["股票代码"]] = row["latest_date"].strftime("%Y%m%d") if row["latest_date"] else None
            
            # 成功执行后提交事务
            conn.commit()
            break  # 成功执行后跳出重试循环
            
        except Exception as e:
            retry_count += 1
            if conn:
                try:
                    conn.rollback()  # 确保回滚事务
                except:
                    pass
            
            if retry_count == max_retries:
                logger.error(f"获取最新日期信息失败(重试{max_retries}次): {str(e)}")
                return {}
            else:
                logger.warning(f"获取最新日期信息失败，正在重试({retry_count}/{max_retries}): {str(e)}")
                
        finally:
            if conn:
                storage._return_connection(conn)
    
    return latest_dates


def ensure_table_exists(conn, table_name, adjust_name, period_name):
    """
    确保表和分区表存在
    
    Args:
        conn: 数据库连接
        table_name (str): 表名
        adjust_name (str): 复权类型
        period_name (str): 频率名称
    """
    # 检查缓存
    cache_key = f"{table_name}_{adjust_name}_{period_name}"
    if cache_key in TABLE_SCHEMA_CACHE:
        return
    
    try:
        # 检查父表是否存在
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = %s AND table_schema = 'public'
            """, [table_name])
            parent_exists = cur.fetchone() is not None
            
            if not parent_exists:
                # 创建父表
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS \"{table_name}\" (
                        \"股票代码\" TEXT,
                        \"日期\" DATE,
                        \"开盘\" DOUBLE PRECISION,
                        \"收盘\" DOUBLE PRECISION,
                        \"最高\" DOUBLE PRECISION,
                        \"最低\" DOUBLE PRECISION,
                        \"成交量\" BIGINT,
                        \"成交额\" DOUBLE PRECISION,
                        \"振幅\" DOUBLE PRECISION,
                        \"涨跌幅\" DOUBLE PRECISION,
                        \"涨跌额\" DOUBLE PRECISION,
                        \"换手率\" DOUBLE PRECISION,
                        \"频率\" TEXT,
                        \"复权类型\" TEXT,
                        \"市场\" TEXT,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (\"股票代码\", \"日期\", \"市场\", \"频率\")
                    ) PARTITION BY LIST (\"市场\");
                """)
        
        # 获取所有市场
        _, market_map = get_stock_codes_from_db()
        markets = set(market_map.values())
        
        # 为每个市场创建分区
        for market in markets:
            market_partition = f"{table_name}_{market}"
            with conn.cursor() as cur:
                # 检查市场分区是否存在
                cur.execute("""
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = %s AND n.nspname = 'public'
                """, [market_partition])
                market_partition_exists = cur.fetchone() is not None
                
                if not market_partition_exists:
                    # 创建市场分区
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS \"{market_partition}\" 
                        PARTITION OF \"{table_name}\" 
                        FOR VALUES IN ('{market}')
                        PARTITION BY LIST (\"频率\");
                    """)
            
            # 为每个频率创建子分区
            freq_partition = f"{market_partition}_{period_name}"
            with conn.cursor() as cur:
                # 检查频率分区是否存在
                cur.execute("""
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = %s AND n.nspname = 'public'
                """, [freq_partition])
                freq_partition_exists = cur.fetchone() is not None
                
                if not freq_partition_exists:
                    # 创建频率分区
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS \"{freq_partition}\" 
                        PARTITION OF \"{market_partition}\" 
                        FOR VALUES IN ('{period_name}');
                    """)
        
        # 更新缓存
        TABLE_SCHEMA_CACHE[cache_key] = True
        
    except Exception as e:
        logger.error(f"确保表存在失败: {str(e)}")
        raise


def copy_data_to_db(conn, df, table_name):
    """
    使用批量插入命令将数据导入数据库
    
    Args:
        conn: 数据库连接
        df (pd.DataFrame): 数据框
        table_name (str): 表名
        
    Returns:
        int: 成功导入的记录数
    """
    if df.empty:
        return 0
    
    # 确保日期列为字符串格式YYYY-MM-DD
    df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
    
    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8') as temp_file:
        # 将数据写入CSV格式的临时文件
        df.to_csv(temp_file.name, sep='\t', header=False, index=False, na_rep='\\N')
        temp_file_path = temp_file.name
    
    try:
        # 使用psycopg的execute_batch方法代替copy_expert
        # 将数据分批处理，使用INSERT
        total_inserted = 0
        for i in range(0, len(df), COPY_BATCH_SIZE):
            batch_df = df.iloc[i:i+COPY_BATCH_SIZE]
            
            # 构建INSERT语句
            columns = ', '.join(f'"{col}"' for col in batch_df.columns)
            placeholders = ', '.join(['%s'] * len(batch_df.columns))
            
            # 构建ON CONFLICT DO UPDATE部分
            update_cols = [col for col in batch_df.columns if col not in ['股票代码', '日期', '市场', '频率']]
            if update_cols:
                # 只包含数据列的更新，不再额外添加updated_at更新
                # 因为表结构中已经有默认值或触发器会更新这个字段
                updates = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in update_cols)
                
                upsert_sql = f"""
                    INSERT INTO \"{table_name}\" ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (\"股票代码\", \"日期\", \"市场\", \"频率\") 
                    DO UPDATE SET {updates}
                """
            else:
                # 如果没有需要更新的列，则什么都不做
                upsert_sql = f"""
                    INSERT INTO \"{table_name}\" ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (\"股票代码\", \"日期\", \"市场\", \"频率\") 
                    DO NOTHING
                """
            
            # 执行UPSERT
            with conn.cursor() as cur:
                # 将DataFrame转换为元组列表
                records = [tuple(x) for x in batch_df.to_numpy()]
                cur.executemany(upsert_sql, records)
            
            total_inserted += len(batch_df)
        
        return total_inserted
    except UniqueViolation:
        # 如果有唯一性冲突，使用ON CONFLICT更新
        logger.warning(f"插入命令遇到唯一性冲突，切换到UPSERT模式")
        
        # 将数据分批处理，使用INSERT ON CONFLICT
        total_inserted = 0
        for i in range(0, len(df), COPY_BATCH_SIZE):
            batch_df = df.iloc[i:i+COPY_BATCH_SIZE]
            
            # 构建INSERT语句
            columns = ', '.join(f'"{col}"' for col in batch_df.columns)
            placeholders = ', '.join(['%s'] * len(batch_df.columns))
            
            # 构建ON CONFLICT DO UPDATE部分
            update_cols = [col for col in batch_df.columns if col not in ['股票代码', '日期', '市场', '频率']]
            if update_cols:
                # 只包含数据列的更新，不再额外添加updated_at更新
                # 因为表结构中已经有默认值或触发器会更新这个字段
                updates = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in update_cols)
                
                upsert_sql = f"""
                    INSERT INTO \"{table_name}\" ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (\"股票代码\", \"日期\", \"市场\", \"频率\") 
                    DO UPDATE SET {updates}
                """
            else:
                # 如果没有需要更新的列，则什么都不做
                upsert_sql = f"""
                    INSERT INTO \"{table_name}\" ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (\"股票代码\", \"日期\", \"市场\", \"频率\") 
                    DO NOTHING
                """
            
            # 执行UPSERT
            with conn.cursor() as cur:
                # 将DataFrame转换为元组列表
                records = [tuple(x) for x in batch_df.to_numpy()]
                cur.executemany(upsert_sql, records)
            
            total_inserted += len(batch_df)
        
        return total_inserted
    finally:
        # 删除临时文件
        try:
            os.remove(temp_file_path)
        except:
            pass


def process_stock_batch_optimized(stock_codes, market_map, period="daily", adjust="hfq", start_date=START_DATE):
    """
    批量处理股票历史数据并保存（优化版）
    
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
    total_records = 0
    adjust_name = ADJUST_TYPE_MAP.get(adjust, "后复权")
    period_name = PERIOD_TYPE_MAP.get(period, "日线")
    
    # 批量获取最新日期
    latest_dates = get_latest_dates_from_db_batch(stock_codes, adjust_name, period_name)
    
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
    
    # 合并所有数据
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        total_records = len(combined_df)
        
        # 批量保存到数据库
        table_name = f"股票行情_{adjust_name}"
        
        conn = storage._get_connection()
        try:
            # 开始事务
            conn.autocommit = False
            
            # 确保表存在
            ensure_table_exists(conn, table_name, adjust_name, period_name)
            
            # 使用COPY命令批量导入数据
            inserted = copy_data_to_db(conn, combined_df, table_name)
            
            # 提交事务
            conn.commit()
            
            logger.info(f"成功导入 {inserted} 条记录到 {table_name}")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"批量导入数据失败: {str(e)}")
            error_count += 1
        finally:
            storage._return_connection(conn)
    
    # 批次处理完成后汇总记录日志
    if error_count > 0:
        # 只记录前5个错误的详细信息
        for i, (code, error_msg) in enumerate(error_stocks[:5]):
            logger.error(f"处理股票 {code} 数据失败: {error_msg}")
        
        if len(error_stocks) > 5:
            logger.error(f"... 还有 {len(error_stocks) - 5} 只股票处理失败 ...")
            
        logger.error(f"批次处理完成: {success_count}只股票成功, {error_count}只失败, 共{total_records}条记录 (复权方式: {adjust_name})")
    
    return success_count


def fetch_all_stock_hist_data_optimized(period="daily", adjust="hfq", max_workers=10, start_date=START_DATE, force_update=False):
    """
    并行获取所有股票的历史行情数据（优化版）
    
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
            logger.info(f"强制更新模式：从 {start_date} 开始获取 {len(stock_codes)} 只股票的{period_name}历史行情数据，复权方式: {adjust_name}")
        else:
            logger.info(f"增量更新模式：获取 {len(stock_codes)} 只股票的最新{period_name}历史行情数据，复权方式: {adjust_name}")
        
        # 处理过程中降低日志级别，减少输出
        logger.setLevel(logging.WARNING)
        
        # 将股票代码列表分成多个批次，每批BATCH_SIZE只股票
        batches = [stock_codes[i:i+BATCH_SIZE] for i in range(0, len(stock_codes), BATCH_SIZE)]
        
        # 使用线程池并行处理
        total_success = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务
            future_to_batch = {executor.submit(process_stock_batch_optimized, batch, market_map, period, adjust, 
                                             start_date if force_update else START_DATE): i 
                             for i, batch in enumerate(batches)}
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_index = future_to_batch[future]
                try:
                    success_count = future.result()
                    total_success += success_count
                except Exception as e:
                    logger.error(f"批次 {batch_index+1}/{len(batches)} 处理失败: {str(e)}")
        
        # 恢复日志级别，显示任务完成信息
        logger.setLevel(logging.INFO)
        logger.info(f"{period_name}历史行情数据获取完成，共成功处理 {total_success} 只股票，复权方式: {adjust_name}")
        return total_success > 0
    except Exception as e:
        # 确保错误信息始终显示
        logger.setLevel(logging.ERROR)
        logger.error(f"获取{period_name}历史行情数据失败: {str(e)}")
        return False
    finally:
        # 确保无论如何都恢复原始日志级别
        logger.setLevel(log_level_before)


def generate_weekly_monthly_data(daily_data, period="weekly"):
    """
    从日频数据生成周频或月频数据
    
    Args:
        daily_data (pd.DataFrame): 日频数据
        period (str): 目标频率，可选值："weekly"(周线)、"monthly"(月线)
        
    Returns:
        pd.DataFrame: 生成的周频或月频数据
    """
    if daily_data.empty:
        return pd.DataFrame()
    
    # 确保日期列为索引且为日期类型
    df = daily_data.copy()
    # 处理可能的NaT值
    df = df.dropna(subset=['日期'])
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
    # 再次过滤掉转换后可能出现的NaT
    df = df.dropna(subset=['日期'])
    df.set_index('日期', inplace=True)
    
    # 设置重采样规则
    if period == "weekly":
        rule = 'W-FRI'  # 每周五作为周线数据点
    elif period == "monthly":
        rule = 'ME'  # 每月最后一天作为月线数据点
    else:
        return pd.DataFrame()
    
    # 按股票代码分组处理
    result_dfs = []
    
    # 使用更高效的分组处理方式
    for code, group in df.groupby('股票代码'):
        try:
            # 对OHLC数据进行重采样
            resampled = group.resample(rule).agg({
                '开盘': 'first',
                '收盘': 'last',
                '最高': 'max',
                '最低': 'min',
                '成交量': 'sum',
                '成交额': 'sum'
            })
            
            # 处理可能的NaN值
            resampled = resampled.dropna(subset=['开盘', '收盘', '最高', '最低'])
            
            # 计算振幅、涨跌幅、涨跌额
            resampled['振幅'] = ((resampled['最高'] - resampled['最低']) / resampled['开盘'].replace(0, np.nan) * 100).round(2)
            resampled['涨跌额'] = (resampled['收盘'] - resampled['开盘']).round(2)
            resampled['涨跌幅'] = ((resampled['收盘'] - resampled['开盘']) / resampled['开盘'].replace(0, np.nan) * 100).round(2)
            
            # 计算换手率（简化处理，直接累加日换手率）
            if '换手率' in group.columns:
                resampled['换手率'] = group['换手率'].resample(rule).sum().round(2)
            
            # 添加股票代码和其他必要字段
            resampled['股票代码'] = code
            
            # 复制其他非OHLC字段
            for col in group.columns:
                if col not in ['开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率', '股票代码']:
                    resampled[col] = group[col].iloc[0]  # 使用第一行的值
            
            # 重置索引，将日期作为列
            resampled.reset_index(inplace=True)
            result_dfs.append(resampled)
        except Exception as e:
            logger.warning(f"处理股票 {code} 的{period}数据时出错: {str(e)}")
            continue
    
    # 合并所有结果
    if result_dfs:
        result = pd.concat(result_dfs, ignore_index=True)
        # 设置频率字段
        result['频率'] = PERIOD_TYPE_MAP.get(period, "周线")
        return result
    
    return pd.DataFrame()


def save_generated_data_optimized(data, period, adjust_name):
    """
    保存生成的周频或月频数据（优化版）
    
    Args:
        data (pd.DataFrame): 数据
        period (str): 频率
        adjust_name (str): 复权类型
        
    Returns:
        bool: 是否成功
    """
    if data.empty:
        return False
        
    try:
        # 获取表名
        table_name = f"股票行情_{adjust_name}"
        period_name = PERIOD_TYPE_MAP.get(period, "周线")
        
        conn = storage._get_connection()
        try:
            # 开始事务
            conn.autocommit = False
            
            # 确保表存在
            ensure_table_exists(conn, table_name, adjust_name, period_name)
            
            # 使用COPY命令批量导入数据
            inserted = copy_data_to_db(conn, data, table_name)
            
            # 提交事务
            conn.commit()
            
            logger.info(f"成功导入 {inserted} 条{period_name}记录到 {table_name}")
            return True
            
        except Exception as e:
            conn.rollback()
            logger.error(f"批量导入{period_name}数据失败: {str(e)}")
            return False
        finally:
            storage._return_connection(conn)
    except Exception as e:
        logger.error(f"保存{period_name}数据失败: {str(e)}")
        return False


def main_optimized(force_update=False, max_workers=10):
    """
    主函数，获取并保存所有股票的历史行情数据（优化版）
    
    Args:
        force_update (bool): 是否强制更新所有数据，忽略数据库中的最新日期
        max_workers (int): 最大并行工作线程数
    
    Returns:
        bool: 是否成功
    """
    try:
        # 确定更新模式
        update_mode = "强制更新" if force_update else "增量更新"
        
        # 提高日志级别，显示重要信息
        log_level_before = logger.level
        logger.setLevel(logging.INFO)
        logger.info(f"开始{update_mode}股票历史行情数据 (优化版)")
        
        # 获取当前日期和时间
        now = datetime.datetime.now()
        today_str = now.strftime("%Y%m%d")
        is_friday = now.weekday() == 4  # 判断今天是否为周五
        is_month_end = (now + datetime.timedelta(days=1)).month != now.month  # 判断今天是否为月末
        
        # 记录开始时间
        start_time = time.time()
        
        # 获取后复权数据（量化研究中普遍采用后复权数据）
        # 1. 先获取日频数据
        success_daily_hfq = fetch_all_stock_hist_data_optimized(
            period="daily", 
            adjust="hfq",
            max_workers=max_workers,
            force_update=force_update
        )
        
        # 2. 获取周频数据
        if is_friday or force_update:
            # 如果今天是周五或强制更新，直接从API获取完整周频数据
            success_weekly_hfq = fetch_all_stock_hist_data_optimized(
                period="weekly", 
                adjust="hfq",
                max_workers=max_workers,
                force_update=force_update
            )
        else:
            # 从日频数据生成周频数据
            # 获取最近3个月的日频数据用于生成
            three_months_ago = (now - datetime.timedelta(days=90)).strftime("%Y%m%d")
            
            # 获取股票代码列表和市场信息
            stock_codes, market_map = get_stock_codes_from_db()
            
            # 批量获取日频数据
            conn = storage._get_connection()
            try:
                # 查询最近3个月的日频数据
                query = """
                    SELECT * FROM "股票行情_后复权"
                    WHERE "频率" = '日线' AND "日期" >= %s
                """
                
                # 使用cursor读取数据，然后转换为DataFrame
                with conn.cursor() as cur:
                    cur.execute(query, [three_months_ago])
                    columns = [desc[0] for desc in cur.description]
                    data = cur.fetchall()
                
                # 将结果转换为DataFrame
                daily_data_hfq = pd.DataFrame(data, columns=columns)
                # 确保日期列为日期类型
                if '日期' in daily_data_hfq.columns and not daily_data_hfq.empty:
                    daily_data_hfq['日期'] = pd.to_datetime(daily_data_hfq['日期'], errors='coerce')
                
                # 生成周频数据
                if not daily_data_hfq.empty:
                    weekly_data_hfq = generate_weekly_monthly_data(daily_data_hfq, period="weekly")
                    success_weekly_hfq = save_generated_data_optimized(weekly_data_hfq, "weekly", "后复权")
                else:
                    success_weekly_hfq = False
                    
            except Exception as e:
                logger.error(f"生成周频数据失败: {str(e)}")
                success_weekly_hfq = False
            finally:
                storage._return_connection(conn)
        
        # 3. 获取月频数据
        if is_month_end or force_update:
            # 如果今天是月末或强制更新，直接从API获取完整月频数据
            success_monthly_hfq = fetch_all_stock_hist_data_optimized(
                period="monthly", 
                adjust="hfq",
                max_workers=max_workers,
                force_update=force_update
            )
        else:
            # 从日频数据生成月频数据
            # 获取最近6个月的日频数据用于生成
            six_months_ago = (now - datetime.timedelta(days=180)).strftime("%Y%m%d")
            
            # 批量获取日频数据
            conn = storage._get_connection()
            try:
                # 查询最近6个月的日频数据
                query = """
                    SELECT * FROM "股票行情_后复权"
                    WHERE "频率" = '日线' AND "日期" >= %s
                """
                
                # 使用cursor读取数据，然后转换为DataFrame
                with conn.cursor() as cur:
                    cur.execute(query, [six_months_ago])
                    columns = [desc[0] for desc in cur.description]
                    data = cur.fetchall()
                
                # 将结果转换为DataFrame
                daily_data_hfq = pd.DataFrame(data, columns=columns)
                # 确保日期列为日期类型
                if '日期' in daily_data_hfq.columns and not daily_data_hfq.empty:
                    daily_data_hfq['日期'] = pd.to_datetime(daily_data_hfq['日期'], errors='coerce')
                
                # 生成月频数据
                if not daily_data_hfq.empty:
                    monthly_data_hfq = generate_weekly_monthly_data(daily_data_hfq, period="monthly")
                    success_monthly_hfq = save_generated_data_optimized(monthly_data_hfq, "monthly", "后复权")
                else:
                    success_monthly_hfq = False
                    
            except Exception as e:
                logger.error(f"生成月频数据失败: {str(e)}")
                success_monthly_hfq = False
            finally:
                storage._return_connection(conn)
        
        # 获取前复权数据
        # 1. 获取日频数据
        success_daily_qfq = fetch_all_stock_hist_data_optimized(
            period="daily", 
            adjust="qfq",
            max_workers=max_workers,
            force_update=force_update
        )
        
        # 2. 获取周频数据
        if is_friday or force_update:
            success_weekly_qfq = fetch_all_stock_hist_data_optimized(
                period="weekly", 
                adjust="qfq",
                max_workers=max_workers,
                force_update=force_update
            )
        else:
            # 从日频数据生成周频数据
            conn = storage._get_connection()
            try:
                # 查询最近3个月的日频数据
                query = """
                    SELECT * FROM "股票行情_前复权"
                    WHERE "频率" = '日线' AND "日期" >= %s
                """
                
                # 使用cursor读取数据，然后转换为DataFrame
                with conn.cursor() as cur:
                    cur.execute(query, [three_months_ago])
                    columns = [desc[0] for desc in cur.description]
                    data = cur.fetchall()
                
                # 将结果转换为DataFrame
                daily_data_qfq = pd.DataFrame(data, columns=columns)
                # 确保日期列为日期类型
                if '日期' in daily_data_qfq.columns and not daily_data_qfq.empty:
                    daily_data_qfq['日期'] = pd.to_datetime(daily_data_qfq['日期'], errors='coerce')
                
                # 生成周频数据
                if not daily_data_qfq.empty:
                    weekly_data_qfq = generate_weekly_monthly_data(daily_data_qfq, period="weekly")
                    success_weekly_qfq = save_generated_data_optimized(weekly_data_qfq, "weekly", "前复权")
                else:
                    success_weekly_qfq = False
                    
            except Exception as e:
                logger.error(f"生成周频数据失败: {str(e)}")
                success_weekly_qfq = False
            finally:
                storage._return_connection(conn)
        
        # 3. 获取月频数据
        if is_month_end or force_update:
            success_monthly_qfq = fetch_all_stock_hist_data_optimized(
                period="monthly", 
                adjust="qfq",
                max_workers=max_workers,
                force_update=force_update
            )
        else:
            # 从日频数据生成月频数据
            conn = storage._get_connection()
            try:
                # 查询最近6个月的日频数据
                query = """
                    SELECT * FROM "股票行情_前复权"
                    WHERE "频率" = '日线' AND "日期" >= %s
                """
                
                # 使用cursor读取数据，然后转换为DataFrame
                with conn.cursor() as cur:
                    cur.execute(query, [six_months_ago])
                    columns = [desc[0] for desc in cur.description]
                    data = cur.fetchall()
                
                # 将结果转换为DataFrame
                daily_data_qfq = pd.DataFrame(data, columns=columns)
                # 确保日期列为日期类型
                if '日期' in daily_data_qfq.columns and not daily_data_qfq.empty:
                    daily_data_qfq['日期'] = pd.to_datetime(daily_data_qfq['日期'], errors='coerce')
                
                # 生成月频数据
                if not daily_data_qfq.empty:
                    monthly_data_qfq = generate_weekly_monthly_data(daily_data_qfq, period="monthly")
                    success_monthly_qfq = save_generated_data_optimized(monthly_data_qfq, "monthly", "前复权")
                else:
                    success_monthly_qfq = False
                    
            except Exception as e:
                logger.error(f"生成月频数据失败: {str(e)}")
                success_monthly_qfq = False
            finally:
                storage._return_connection(conn)
        
        # 判断整体是否成功
        success_hfq = success_daily_hfq and success_weekly_hfq and success_monthly_hfq
        success_qfq = success_daily_qfq and success_weekly_qfq and success_monthly_qfq
        success = success_hfq and success_qfq
        
        # 计算总耗时
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        # 恢复日志级别，显示任务完成信息
        logger.setLevel(logging.INFO)
        
        if success:
            logger.info(f"股票历史行情数据（{update_mode}模式）获取并保存成功，总耗时: {elapsed_time:.2f}秒")
            return True
        else:
            logger.error(f"部分股票历史行情数据获取或保存失败，总耗时: {elapsed_time:.2f}秒")
            return False
    except Exception as e:
        # 恢复日志级别，确保错误信息被显示
        logger.setLevel(logging.INFO)
        logger.error(f"股票历史行情数据获取或保存过程中出错: {str(e)}")
        return False
    finally:
        # 确保无论如何都恢复原始日志级别
        logger.setLevel(log_level_before)


def create_sql_functions():
    """
    创建SQL函数用于数据处理和聚合
    """
    conn = storage._get_connection()
    try:
        with conn.cursor() as cur:
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
        logger.info("成功创建SQL函数和索引")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"创建SQL函数失败: {str(e)}")
        return False
    finally:
        storage._return_connection(conn)


def generate_period_data_with_sql(period="weekly", adjust="hfq", days_back=90):
    """
    使用SQL函数生成周频或月频数据
    
    Args:
        period (str): 目标频率，可选值："weekly"(周线)、"monthly"(月线)
        adjust (str): 复权方式，可选值：""(不复权)、"qfq"(前复权)、"hfq"(后复权)
        days_back (int): 向前获取多少天的数据
        
    Returns:
        bool: 是否成功
    """
    conn = storage._get_connection()
    try:
        # 计算日期范围
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        adjust_name = ADJUST_TYPE_MAP.get(adjust, "后复权")
        function_name = "generate_weekly_data" if period == "weekly" else "generate_monthly_data"
        
        # 调用SQL函数生成数据
        with conn.cursor() as cur:
            cur.execute(f"SELECT {function_name}(%s, %s, %s)", [start_date, end_date, adjust_name])
        
        conn.commit()
        logger.info(f"成功使用SQL生成{PERIOD_TYPE_MAP.get(period, '周线')}数据，复权方式: {adjust_name}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"使用SQL生成{PERIOD_TYPE_MAP.get(period, '周线')}数据失败: {str(e)}")
        return False
    finally:
        storage._return_connection(conn)


if __name__ == "__main__":
    # 设置日志级别 - 调整为WARNING级别以减少输出
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 只有关键信息才会显示
    logger.setLevel(logging.WARNING)
    
    # 创建SQL函数
    create_sql_functions()
    
    # 默认使用增量更新模式
    # 如需强制更新所有数据，可以传入force_update=True
    main_optimized(force_update=False, max_workers=10)