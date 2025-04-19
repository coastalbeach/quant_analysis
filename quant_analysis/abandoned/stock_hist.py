#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票历史行情数据获取模块

功能：
1. 从基础表获取股票代码列表
2. 使用akshare的stock_zh_a_hist接口获取2005年1月4日以后的全部历史行情数据
3. 支持日线、周线、月线三种频率数据获取
4. 支持前复权、后复权数据分表存储

"""

import logging
import pandas as pd
import akshare as ak
import datetime
import concurrent.futures
from psycopg import sql
from core.storage import storage
import time
import random
from tqdm import tqdm

# 初始化日志
logger = logging.getLogger('quant.fetcher.hist.stock_hist')

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


def get_stock_codes_from_db():
    """
    从基础表获取股票代码和市场信息
    
    Returns:
        tuple: (股票代码列表, 股票代码到市场的映射字典)
    """
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
        
        # 注意：akshare接口已经返回了"股票代码"字段，无需添加
        # 确保日期列为日期类型
        df['日期'] = pd.to_datetime(df['日期'])
        
        # 添加频率字段
        df['频率'] = PERIOD_TYPE_MAP.get(period, "日线")
        
        # 保留原始中文列名，不进行修饰
        # 不再为每只股票单独记录日志，改为在批处理中汇总记录
        
        return df
    except Exception as e:
        # 只记录错误日志，减少日志输出
        logger.error(f"获取股票 {stock_code} 的{PERIOD_TYPE_MAP.get(period, '日线')}历史行情数据失败: {str(e)}")
        return pd.DataFrame()


def get_latest_dates_from_db(stock_codes, adjust_name, period_name="日线", market_map=None):
    """
    批量获取股票在数据库中的最新日期
    
    Args:
        stock_codes (list): 股票代码列表
        adjust_name (str): 复权类型名称（后复权、前复权、原始）
        period_name (str): 频率名称（日线、周线、月线），默认为日线
        market_map (dict, optional): 股票代码到市场的映射字典，用于按市场分区查询
        
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
            
            # 如果提供了市场信息，按市场分组查询以提高效率
            if market_map:
                # 按市场分组股票代码
                market_stocks = {}
                for code in stock_codes:
                    market = market_map.get(code)
                    if market:
                        if market not in market_stocks:
                            market_stocks[market] = []
                        market_stocks[market].append(code)
                
                # 对每个市场分别查询
                for market, codes in market_stocks.items():
                    if not codes:
                        continue
                    
                    # 使用参数化查询和psycopg3的SQL构建
                    from psycopg.sql import SQL, Identifier
                    table_name = f"股票行情_{adjust_name}_{market}_{period_name}"
                    query = SQL("""
                        SELECT "股票代码", MAX("日期") as latest_date 
                        FROM {} 
                        WHERE "股票代码" = ANY(%s)
                        GROUP BY "股票代码"
                    """).format(Identifier(table_name))
                    
                    with conn.cursor() as cur:
                        cur.execute(query, [codes])
                        results = cur.fetchall()
                        for row in results:
                            # 将日期转换为字符串格式YYYYMMDD
                            latest_dates[row["股票代码"]] = row["latest_date"].strftime("%Y%m%d") if row["latest_date"] else None
            else:
                # 如果没有市场信息，从父表查询
                table_name = f"股票行情_{adjust_name}"
                query = SQL("""
                    SELECT "股票代码", MAX("日期") as latest_date 
                    FROM {} 
                    WHERE "股票代码" = ANY(%s) AND "频率" = %s
                    GROUP BY "股票代码"
                """).format(Identifier(table_name))
                
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
    success_count = 0
    error_count = 0
    total_records = 0
    adjust_name = ADJUST_TYPE_MAP.get(adjust, "后复权")
    period_name = PERIOD_TYPE_MAP.get(period, "日线")
    
    # 批量获取最新日期（按市场和频率分区查询）
    latest_dates = get_latest_dates_from_db(stock_codes, adjust_name, period_name, market_map)
    
    # 使用tqdm创建进度条，禁用日志输出以避免与进度条冲突
    log_level_before = logger.level
    logger.setLevel(logging.ERROR)  # 临时提高日志级别，只输出错误
    
    # 收集错误信息，而不是立即输出
    error_stocks = []
    
    # 使用tqdm创建进度条
    for stock_code in tqdm(stock_codes, desc=f"处理{period_name}数据({adjust_name})"):
        try:
            # 确定获取数据的起始日期
            fetch_start_date = start_date
            if stock_code in latest_dates and latest_dates[stock_code]:
                # 直接使用表内最后日期作为获取数据起始日期
                # akshare接口会自动处理差异数据
                fetch_start_date = latest_dates[stock_code]
            
            # 获取历史行情数据
            df = fetch_stock_hist_data(stock_code, period, adjust, fetch_start_date)
            if df.empty:
                # 可能是因为没有新数据，也算成功
                if stock_code in latest_dates and latest_dates[stock_code]:
                    success_count += 1
                continue
                
            # 添加复权类型字段（fetch_stock_hist_data已添加频率字段）
            df['复权类型'] = adjust_name
            
            # 添加市场字段（从基础表获取的映射）
            if stock_code in market_map:
                df['市场'] = market_map[stock_code]
            
            # 记录总记录数
            total_records += len(df)
            
            # 保存到数据库（使用新的分区表结构）
            storage.save(
                df=df,
                data_type="股票行情",
                frequency=period,
                primary_keys=["股票代码", "日期", "市场", "频率"],
                replace=False,  # 使用增量更新模式
                adjust_type=adjust_name
            )
            
            success_count += 1
            
        except Exception as e:
            error_count += 1
            # 收集错误信息而不是立即输出
            error_stocks.append((stock_code, str(e)))
    
    # 恢复之前的日志级别
    logger.setLevel(log_level_before)
    
    # 批次处理完成后汇总记录日志，只输出错误数量和总结信息，不输出每个错误的详细信息
    if error_count > 0:
        # 只记录前5个错误的详细信息，避免日志过多
        for i, (code, error_msg) in enumerate(error_stocks[:5]):
            logger.error(f"处理股票 {code} 数据失败: {error_msg}")
        
        if len(error_stocks) > 5:
            logger.error(f"... 还有 {len(error_stocks) - 5} 只股票处理失败 ...")
            
        logger.error(f"批次处理完成: {success_count}只股票成功, {error_count}只失败, 共{total_records}条记录 (复权方式: {adjust_name})")
    
    return success_count


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
    df['日期'] = pd.to_datetime(df['日期'])
    df.set_index('日期', inplace=True)
    
    # 设置重采样规则
    if period == "weekly":
        rule = 'W-FRI'  # 每周五作为周线数据点
    elif period == "monthly":
        rule = 'ME'  # 每月最后一天作为月线数据点（使用ME替代已弃用的M）
    else:
        return pd.DataFrame()
    
    # 按股票代码分组处理
    result_dfs = []
    for code, group in df.groupby('股票代码'):
        # 对OHLC数据进行重采样
        resampled = group.resample(rule).agg({
            '开盘': 'first',
            '收盘': 'last',
            '最高': 'max',
            '最低': 'min',
            '成交量': 'sum',
            '成交额': 'sum'
        })
        
        # 计算振幅、涨跌幅、涨跌额
        resampled['振幅'] = ((resampled['最高'] - resampled['最低']) / resampled['开盘'] * 100).round(2)
        resampled['涨跌额'] = (resampled['收盘'] - resampled['开盘']).round(2)
        resampled['涨跌幅'] = ((resampled['收盘'] - resampled['开盘']) / resampled['开盘'] * 100).round(2)
        
        # 计算换手率（简化处理，直接累加日换手率）
        if '换手率' in group.columns:
            # 使用与rule相同的频率规则进行重采样
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
    
    # 合并所有结果
    if result_dfs:
        result = pd.concat(result_dfs, ignore_index=True)
        # 设置频率字段
        result['频率'] = PERIOD_TYPE_MAP.get(period, "周线")
        return result
    
    return pd.DataFrame()


def fetch_all_stock_hist_data(period="daily", adjust="hfq", max_workers=50, start_date=START_DATE, force_update=False):
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
            logger.info(f"强制更新模式：从 {start_date} 开始获取 {len(stock_codes)} 只股票的{period_name}历史行情数据，复权方式: {adjust_name}")
        else:
            logger.info(f"增量更新模式：获取 {len(stock_codes)} 只股票的最新{period_name}历史行情数据，复权方式: {adjust_name}")
        
        # 处理过程中降低日志级别，减少输出
        logger.setLevel(logging.WARNING)
        
        # 将股票代码列表分成多个批次
        batch_size = max(1, len(stock_codes) // max_workers)
        batches = [stock_codes[i:i+batch_size] for i in range(0, len(stock_codes), batch_size)]
        
        # 使用线程池并行处理
        total_success = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务，同时传递市场信息
            # 如果强制更新，使用指定的起始日期；否则使用增量更新
            future_to_batch = {executor.submit(process_stock_batch, batch, market_map, period, adjust, 
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


def save_generated_data(data, period, adjust_name):
    """
    保存生成的周频或月频数据
    
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
        # 保存到数据库
        storage.save(
            df=data,
            data_type="股票行情",
            frequency=period,
            primary_keys=["股票代码", "日期", "市场", "频率"],
            replace=False,  # 使用增量更新模式
            adjust_type=adjust_name
        )
        return True
    except Exception as e:
        logger.error(f"保存{PERIOD_TYPE_MAP.get(period, '周线')}数据失败: {str(e)}")
        return False


def is_complete_trading_period(date, period="weekly"):
    """
    判断给定日期是否为完整交易周期的结束日期
    
    Args:
        date (datetime.datetime): 日期
        period (str): 周期类型，可选值："weekly"(周线)、"monthly"(月线)
        
    Returns:
        bool: 是否为完整交易周期的结束日期
    """
    date = pd.to_datetime(date)
    
    if period == "weekly":
        # 判断是否为周五（Python中周一是0，周日是6）
        return date.weekday() == 4  # 周五
    
    elif period == "monthly":
        # 判断是否为月末最后一个交易日
        # 简化处理：判断是否为月末
        next_day = date + pd.Timedelta(days=1)
        return date.month != next_day.month
    
    return False


def get_period_range(start_date, end_date, period="weekly"):
    """
    获取指定时间范围内的完整交易周期数量
    
    Args:
        start_date (str): 开始日期，格式：YYYYMMDD
        end_date (str): 结束日期，格式：YYYYMMDD
        period (str): 周期类型，可选值："weekly"(周线)、"monthly"(月线)
        
    Returns:
        int: 完整交易周期数量
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    if period == "weekly":
        # 计算周数，考虑不完整的周
        weeks = (end - start).days // 7
        # 额外检查是否跨越了周五
        start_weekday = start.weekday()
        end_weekday = end.weekday()
        if start_weekday <= 4 and end_weekday >= 4:  # 如果开始日在周五之前，结束日在周五之后，额外计算一周
            weeks += 1
        return max(0, weeks)
    
    elif period == "monthly":
        # 计算月数
        months = (end.year - start.year) * 12 + end.month - start.month
        # 如果开始日期的天数大于结束日期的天数，减少一个月
        if start.day > end.day:
            months -= 1
        return max(0, months)
    
    return 0


def fetch_and_generate_data(adjust="hfq", period="daily", start_date=START_DATE, end_date=END_DATE, force_update=False, max_workers=50):
    """
    获取并生成指定复权类型和频率的股票历史行情数据
    
    Args:
        adjust (str): 复权方式，可选值：""(不复权)、"qfq"(前复权)、"hfq"(后复权)
        period (str): 数据频率，可选值："daily"(日线)、"weekly"(周线)、"monthly"(月线)
        start_date (str): 开始日期，默认为全局设置的START_DATE
        end_date (str): 结束日期，默认为当前日期
        force_update (bool): 是否强制更新所有数据，忽略数据库中的最新日期
        max_workers (int): 最大并行工作线程数
        
    Returns:
        tuple: (是否成功, 获取的数据)
    """
    # 保存当前日志级别，以便后续恢复
    log_level_before = logger.level
    
    try:
        adjust_name = ADJUST_TYPE_MAP.get(adjust, "后复权")
        period_name = PERIOD_TYPE_MAP.get(period, "日线")
        
        # 临时提高日志级别以显示重要信息
        logger.setLevel(logging.INFO)
        logger.info(f"开始获取{period_name}历史行情数据，复权方式: {adjust_name}")
        
        # 处理过程中降低日志级别，减少输出
        logger.setLevel(logging.WARNING)
        
        # 获取股票代码列表和市场信息
        stock_codes, market_map = get_stock_codes_from_db()
        if not stock_codes:
            logger.setLevel(logging.ERROR)
            logger.error("未获取到股票代码，无法继续获取历史行情数据")
            return False, None
        
        # 判断是否需要直接获取API数据或从日频数据生成
        if period in ["weekly", "monthly"]:
            # 检查日期范围内是否有完整的周期
            period_count = get_period_range(start_date, end_date, period)
            
            if period_count >= 2 or force_update:
                # 如果有2个及以上完整周期或强制更新，直接从API获取数据
                success = fetch_all_stock_hist_data(
                    period=period,
                    adjust=adjust,
                    max_workers=max_workers,
                    start_date=start_date,
                    force_update=force_update
                )
                return success, None  # 数据已直接保存到数据库，不需要返回
            else:
                # 如果没有完整周期或只有1个完整周期，从日频数据生成
                # 先获取日频数据
                success_daily, daily_data = fetch_and_generate_data(
                    adjust=adjust,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    force_update=force_update,
                    max_workers=max_workers
                )
                
                if not success_daily or daily_data is None or daily_data.empty:
                    logger.error(f"无法获取日频数据，无法生成{period_name}数据")
                    return False, None
                
                # 从日频数据生成周频或月频数据
                generated_data = generate_weekly_monthly_data(daily_data, period=period)
                if generated_data.empty:
                    logger.error(f"生成{period_name}数据失败")
                    return False, None
                
                # 保存生成的数据
                success = save_generated_data(generated_data, period, adjust_name)
                return success, generated_data
        else:  # 日频数据直接从API获取
            success = fetch_all_stock_hist_data(
                period=period,
                adjust=adjust,
                max_workers=max_workers,
                start_date=start_date,
                force_update=force_update
            )
            
            # 如果成功，获取最近一段时间的数据用于生成周频和月频数据
            if success:
                # 获取最近3个月的数据，足够生成周频和月频数据
                three_months_ago = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime("%Y%m%d")
                
                # 获取股票代码列表和市场信息
                stock_codes, market_map = get_stock_codes_from_db()
                if not stock_codes:
                    return True, None  # 已经成功获取并保存了数据，但无法返回数据用于生成
                
                # 将股票代码列表分成多个批次
                batch_size = max(1, len(stock_codes) // max_workers)
                batches = [stock_codes[i:i+batch_size] for i in range(0, len(stock_codes), batch_size)]
                
                # 收集所有数据
                all_data = []
                
                # 使用线程池并行获取数据（不保存）
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_batch = {}
                    for batch in batches:
                        for stock_code in batch:
                            future = executor.submit(
                                fetch_stock_hist_data,
                                stock_code,
                                period="daily",
                                adjust=adjust,
                                start_date=three_months_ago
                            )
                            future_to_batch[future] = stock_code
                    
                    for future in concurrent.futures.as_completed(future_to_batch):
                        stock_code = future_to_batch[future]
                        try:
                            df = future.result()
                            if not df.empty:
                                # 添加复权类型字段
                                df['复权类型'] = adjust_name
                                # 添加市场字段
                                if stock_code in market_map:
                                    df['市场'] = market_map[stock_code]
                                all_data.append(df)
                        except Exception as e:
                            logger.error(f"获取股票 {stock_code} 数据失败: {str(e)}")
                
                # 合并所有数据
                if all_data:
                    combined_data = pd.concat(all_data, ignore_index=True)
                    return True, combined_data
                else:
                    logger.warning("未获取到任何数据用于生成周频和月频数据")
                    return True, None
            
            return success, None
    
    except Exception as e:
        # 确保错误信息始终显示
        logger.setLevel(logging.ERROR)
        logger.error(f"获取{period_name}历史行情数据失败: {str(e)}")
        return False, None
    finally:
        # 确保无论如何都恢复原始日志级别
        logger.setLevel(log_level_before)


def main(force_update=False):
    """
    主函数，获取并保存所有股票的历史行情数据
    
    Args:
        force_update (bool): 是否强制更新所有数据，忽略数据库中的最新日期
    
    Returns:
        bool: 是否成功
    """
    try:
        # 确定更新模式
        update_mode = "强制更新" if force_update else "增量更新"
        
        # 提高日志级别，显示重要信息
        log_level_before = logger.level
        logger.setLevel(logging.INFO)
        logger.info(f"开始{update_mode}股票历史行情数据")
        # 处理过程中恢复到WARNING级别，减少输出
        logger.setLevel(logging.WARNING)
        
        # 获取当前日期和时间
        now = datetime.datetime.now()
        today_str = now.strftime("%Y%m%d")
        is_friday = now.weekday() == 4  # 判断今天是否为周五
        is_month_end = (now + datetime.timedelta(days=1)).month != now.month  # 判断今天是否为月末
        
        # 获取后复权数据（量化研究中普遍采用后复权数据）
        # 1. 先获取日频数据
        success_daily_hfq, daily_data_hfq = fetch_and_generate_data(
            adjust="hfq", 
            period="daily",
            end_date=today_str,
            force_update=force_update
        )
        
        # 2. 获取周频数据
        # 如果今天是周五，直接从API获取完整周频数据
        # 否则，从日频数据生成周频数据
        if is_friday or force_update:
            success_weekly_hfq, _ = fetch_and_generate_data(
                adjust="hfq", 
                period="weekly",
                end_date=today_str,
                force_update=force_update
            )
        elif success_daily_hfq and daily_data_hfq is not None:
            # 从日频数据生成周频数据
            weekly_data_hfq = generate_weekly_monthly_data(daily_data_hfq, period="weekly")
            success_weekly_hfq = save_generated_data(weekly_data_hfq, "weekly", "后复权")
        else:
            success_weekly_hfq = False
        
        # 3. 获取月频数据
        # 如果今天是月末，直接从API获取完整月频数据
        # 否则，从日频数据生成月频数据
        if is_month_end or force_update:
            success_monthly_hfq, _ = fetch_and_generate_data(
                adjust="hfq", 
                period="monthly",
                end_date=today_str,
                force_update=force_update
            )
        elif success_daily_hfq and daily_data_hfq is not None:
            # 从日频数据生成月频数据
            monthly_data_hfq = generate_weekly_monthly_data(daily_data_hfq, period="monthly")
            success_monthly_hfq = save_generated_data(monthly_data_hfq, "monthly", "后复权")
        else:
            success_monthly_hfq = False
        
        # 获取前复权数据 - 获取全部历史数据
        # 1. 先获取日频数据
        success_daily_qfq, daily_data_qfq = fetch_and_generate_data(
            adjust="qfq", 
            period="daily",
            end_date=today_str,
            force_update=force_update
        )
        
        # 2. 获取周频数据
        if is_friday or force_update:
            success_weekly_qfq, _ = fetch_and_generate_data(
                adjust="qfq", 
                period="weekly",
                end_date=today_str,
                force_update=force_update
            )
        elif success_daily_qfq and daily_data_qfq is not None:
            # 从日频数据生成周频数据
            weekly_data_qfq = generate_weekly_monthly_data(daily_data_qfq, period="weekly")
            success_weekly_qfq = save_generated_data(weekly_data_qfq, "weekly", "前复权")
        else:
            success_weekly_qfq = False
        
        # 3. 获取月频数据
        if is_month_end or force_update:
            success_monthly_qfq, _ = fetch_and_generate_data(
                adjust="qfq", 
                period="monthly",
                end_date=today_str,
                force_update=force_update
            )
        elif success_daily_qfq and daily_data_qfq is not None:
            # 从日频数据生成月频数据
            monthly_data_qfq = generate_weekly_monthly_data(daily_data_qfq, period="monthly")
            success_monthly_qfq = save_generated_data(monthly_data_qfq, "monthly", "前复权")
        else:
            success_monthly_qfq = False
        
        # 判断整体是否成功
        success_hfq = success_daily_hfq and success_weekly_hfq and success_monthly_hfq
        success_qfq = success_daily_qfq and success_weekly_qfq and success_monthly_qfq
        success = success_hfq and success_qfq
        
        # 恢复日志级别，显示任务完成信息
        logger.setLevel(logging.INFO)
        
        if success:
            logger.info(f"股票历史行情数据（{update_mode}模式）获取并保存成功")
            return True
        else:
            logger.error("部分股票历史行情数据获取或保存失败")
            return False
    except Exception as e:
        # 恢复日志级别，确保错误信息被显示
        logger.setLevel(logging.INFO)
        logger.error(f"股票历史行情数据获取或保存过程中出错: {str(e)}")
        return False
    finally:
        # 确保无论如何都恢复原始日志级别
        logger.setLevel(log_level_before)


if __name__ == "__main__":
    # 设置日志级别 - 调整为WARNING级别以减少输出
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 只有关键信息才会显示
    logger.setLevel(logging.WARNING)
    
    # 默认使用增量更新模式
    # 如需强制更新所有数据，可以传入force_update=True
    main(force_update=False)