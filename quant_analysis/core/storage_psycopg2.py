"""PostgreSQL数据存储抽象层
功能特性：
1. 自动表结构推断与动态建表
2. 数据去重与增量更新机制
3. 支持多种频率数据的统一处理
4. 事务管理与批量写入优化
5. 支持自定义主键和表结构变更
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
import yaml
from psycopg2 import pool, sql, Error as PgError
from psycopg2.extras import execute_batch

# 初始化日志
logger = logging.getLogger('quant.storage')

class DatabaseError(Exception):
    """数据库操作异常"""
    pass

class QuantStorage:
    """量化数据存储核心类"""
    
    def __init__(self, config_path='config/db.yaml'):
        self._load_config(config_path)
        self._init_connection_pool()
        self.type_map = {
            'object': 'TEXT',
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN',
            'category': 'TEXT'
        }

    def _load_config(self, config_path: str) -> None:
        """加载数据库配置"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                self.db_config = config['postgresql']
                self.pool_config = {
                    'minconn': config.get('pool', {}).get('minconn', 1),
                    'maxconn': config.get('pool', {}).get('maxconn', 10)
                }
        except Exception as e:
            raise DatabaseError(f"配置加载失败: {str(e)}")

    def _init_connection_pool(self) -> None:
        """初始化数据库连接池"""
        try:
            self.pool = pool.SimpleConnectionPool(
                minconn=self.pool_config['minconn'],
                maxconn=self.pool_config['maxconn'],
                **self.db_config
            )
        except PgError as e:
            raise DatabaseError(f"连接池初始化失败: {str(e)}")

    def _get_connection(self):
        """获取数据库连接"""
        try:
            return self.pool.getconn()
        except PgError as e:
            raise DatabaseError(f"获取数据库连接失败: {str(e)}")

    def _return_connection(self, conn) -> None:
        """归还连接到连接池"""
        self.pool.putconn(conn)
        
    def _get_market_from_basic_table(self, stock_codes: List[str]) -> Dict[str, str]:
        """从基础表获取股票的市场信息
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            Dict[str, str]: 股票代码到市场的映射
        """
        if not stock_codes:
            return {}
            
        market_map = {}
        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                # 构建IN查询的参数
                placeholders = ", ".join(["'" + code + "'" for code in stock_codes])
                query = f"SELECT 代码, 市场 FROM 基础 WHERE 代码 IN ({placeholders})"
                cur.execute(query)
                results = cur.fetchall()
                market_map = {row[0]: row[1] for row in results}
                return market_map
        except Exception as e:
            logger.error(f"获取市场信息失败: {str(e)}")
            return {}
        finally:
            self._return_connection(conn)

    def _generate_table_name(self, data_type: str, frequency: str = None, adjust_type: str = None) -> str:
        """动态生成表名规则
        
        根据新的数据库结构，使用以下规则：
        1. 对于股票行情数据，按复权类型分为不同的表：股票行情_前复权、股票行情_后复权
        2. 其他数据类型直接使用中文表名
        
        Args:
            data_type: 数据类型（基础, 股票行情, 板块行情, 指数行情等）
            frequency: 数据频率（daily, weekly, monthly）
            adjust_type: 复权类型（前复权, 后复权）
            
        Returns:
            str: 生成的表名
        """
        # 将原来的'行情'数据类型映射为'股票行情'
        if data_type == '行情':
            data_type = '股票行情'
            
        # 如果是股票行情数据且指定了复权类型，则按复权类型分表
        if data_type == '股票行情' and adjust_type in ['前复权', '后复权']:
            return f"{data_type}_{adjust_type}"
        
        # 其他情况直接返回数据类型作为表名
        return data_type

    def _infer_table_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """推断表结构
        
        对于股票历史数据，日期字段统一使用DATE类型，不需要精确到时分秒
        """
        schema = {}
        for col, dtype in df.dtypes.items():
            pg_type = self.type_map.get(str(dtype), 'TEXT')
            # 对于日期字段，统一使用DATE类型，简化处理
            if 'datetime' in str(dtype) or col == '日期':
                pg_type = 'DATE'
            schema[col] = pg_type
        return schema

    def _get_existing_columns(self, table_name: str, conn) -> Dict[str, str]:
        """获取现有表结构"""
        query = sql.SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = {table}
        """).format(table=sql.Literal(table_name))
        
        with conn.cursor() as cur:
            cur.execute(query)
            return {row[0]: row[1] for row in cur.fetchall()}

    def _alter_table(self, table_name: str, new_columns: Dict[str, str], conn) -> None:
        """添加新列到现有表"""
        for col, dtype in new_columns.items():
            alter_query = sql.SQL("""
                ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {type}
            """).format(
                table=sql.Identifier(table_name),
                column=sql.Identifier(col),
                type=sql.SQL(dtype)
            )
            with conn.cursor() as cur:
                cur.execute(alter_query)

    def _create_table(self, table_name: str, schema: Dict[str, str], 
                      primary_keys: List[str], conn) -> None:
        """动态创建数据表，支持PostgreSQL 16原生分区
        
        对于分区表，使用以下分区策略：
        1. 父表按市场分区
        2. 子表按频率分区
        
        注意：分区表的主键必须包含所有分区列
        """
        columns = [sql.Identifier(col) + sql.SQL(" " + dtype)
                  for col, dtype in schema.items()]
        
        # 添加系统字段
        columns.extend([
            sql.SQL("created_at TIMESTAMP DEFAULT NOW()"),
            sql.SQL("updated_at TIMESTAMP DEFAULT NOW()")
        ])
        
        # 检查是否为需要分区的表
        is_partition_table = '市场' in schema and '频率' in schema
        
        # 检查是否为股票行情表（用于日志和其他特殊处理）
        is_quote_table = table_name.startswith('股票行情_前复权') or table_name.startswith('股票行情_后复权')
        
        # 如果需要分区，确保主键包含分区列
        if is_partition_table:
            # 确保主键包含分区列
            partition_keys = ['市场', '频率']
            for key in partition_keys:
                if key not in primary_keys:
                    primary_keys.append(key)
            
            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {columns},
                    PRIMARY KEY ({pkey})
                ) PARTITION BY LIST ("市场");
            """).format(
                table=sql.Identifier(table_name),
                columns=sql.SQL(',\n').join(columns),
                pkey=sql.SQL(', ').join(map(sql.Identifier, primary_keys))
            )
            
            with conn.cursor() as cur:
                cur.execute(query)
        else:
            # 对于非分区表，创建普通表
            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {columns},
                    PRIMARY KEY ({pkey})
                )
            """).format(
                table=sql.Identifier(table_name),
                columns=sql.SQL(',\n').join(columns),
                pkey=sql.SQL(', ').join(map(sql.Identifier, primary_keys))
            )
            
            with conn.cursor() as cur:
                cur.execute(query)

    def _ensure_partition_exists(self, table_name: str, market: str, frequency: str, conn) -> None:
        """确保分区表存在，如果不存在则创建
        
        Args:
            table_name: 父表名称（如'股票行情_后复权'）
            market: 市场名称
            frequency: 数据频率
            conn: 数据库连接
        """
        # 分区表命名规则：父表名_市场_频率
        partition_name = f"{table_name}_{market}_{frequency}"
        
        # 检查分区是否存在
        check_query = """
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %s AND n.nspname = 'public'
        """
        
        with conn.cursor() as cur:
            cur.execute(check_query, (partition_name,))
            exists = cur.fetchone() is not None
            
            if not exists:
                # 创建分区表
                create_partition_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {partition} PARTITION OF {parent}
                    FOR VALUES IN ({market_value})
                    PARTITION BY LIST ("频率");
                """).format(
                    partition=sql.Identifier(f"{table_name}_{market}"),
                    parent=sql.Identifier(table_name),
                    market_value=sql.Literal(market)
                )
                cur.execute(create_partition_query)
                
                # 创建子分区
                create_subpartition_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {subpartition} PARTITION OF {partition}
                    FOR VALUES IN ({frequency_value});
                """).format(
                    subpartition=sql.Identifier(partition_name),
                    partition=sql.Identifier(f"{table_name}_{market}"),
                    frequency_value=sql.Literal(frequency)
                )
                cur.execute(create_subpartition_query)
                logger.info(f"创建分区表: {partition_name}")

    def save(self,
             df: pd.DataFrame,
             data_type: str,
             frequency: str = None,
             primary_keys: Optional[List[str]] = None,
             replace: bool = False,
             adjust_type: str = None) -> None:
        """主存储方法
        :param df: 待存储数据框
        :param data_type: 数据类型（基础, 行情, 行业板块, 概念板块...）
        :param frequency: 数据频率（daily, weekly, monthly）
        :param primary_keys: 主键字段列表，默认为['代码', '日期']
        :param replace: 是否全量替换
        :param adjust_type: 复权类型（前复权, 后复权）
        """
        if df.empty:
            logger.warning("数据为空，跳过存储")
            return

        # 标准化处理
        primary_keys = primary_keys or ['代码', '日期']
        
        # 验证主键字段存在
        missing_keys = [key for key in primary_keys if key not in df.columns]
        if missing_keys:
            raise ValueError(f"缺少主键字段: {missing_keys}")

        # 获取表名
        table_name = self._generate_table_name(data_type, frequency, adjust_type)
        schema = self._infer_table_schema(df)
        
        # 对于行情表，需要确保市场字段存在，用于分区
        if data_type == '行情' and '市场' not in df.columns:
            # 如果没有市场字段，尝试从基础表获取
            if '股票代码' in df.columns:
                market_map = self._get_market_from_basic_table(df['股票代码'].tolist())
                if market_map:
                    df['市场'] = df['股票代码'].map(market_map)
                else:
                    logger.warning("无法获取市场信息，可能影响行情数据存储")
            elif '代码' in df.columns:
                market_map = self._get_market_from_basic_table(df['代码'].tolist())
                if market_map:
                    df['市场'] = df['代码'].map(market_map)
                else:
                    logger.warning("无法获取市场信息，可能影响行情数据存储")
            else:
                logger.warning("行情数据缺少市场字段，可能影响分区存储")
        
        # 对于行情表，添加频率字段
        if data_type == '行情' and frequency:
            freq_map = {
                'daily': '日线',
                'weekly': '周线',
                'monthly': '月线'
            }
            df['频率'] = freq_map.get(frequency, '日线')
        
        conn = self._get_connection()
        try:
            conn.autocommit = False
            
            # 检查表是否存在，不存在则创建
            self._create_table(table_name, schema, primary_keys, conn)
            
            # 检查表结构变更
            existing_columns = self._get_existing_columns(table_name, conn)
            new_columns = {k: v for k, v in schema.items() if k not in existing_columns}
            if new_columns:
                self._alter_table(table_name, new_columns, conn)
            
            # 如果是股票行情表且有市场和频率信息，确保分区存在
            is_quote_table = table_name.startswith('股票行情_前复权') or table_name.startswith('股票行情_后复权')
            if is_quote_table and '市场' in df.columns and '频率' in df.columns:
                # 按市场和频率分组处理数据
                for (market, freq), group_df in df.groupby(['市场', '频率']):
                    if not group_df.empty:
                        # 确保分区存在
                        self._ensure_partition_exists(table_name, market, freq, conn)
            
            # 转换时间字段 - 简化处理，对于股票历史数据只需要年月日
            datetime_cols = [k for k, v in schema.items() if v in ['DATE', 'TIMESTAMP']]
            for col in datetime_cols:
                # 对于日期列，只保留年月日，不需要时分秒
                if col == '日期' or schema[col] == 'DATE':
                    df[col] = pd.to_datetime(df[col]).dt.date
                else:
                    # 其他时间戳字段保持原有处理方式
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

            # 生成写入SQL
            columns = sql.SQL(', ').join(map(sql.Identifier, df.columns))
            placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(df.columns))
            
            if replace:
                insert_sql = sql.SQL("""
                    INSERT INTO {table} ({columns})
                    VALUES ({values})
                """).format(
                    table=sql.Identifier(table_name),
                    columns=columns,
                    values=placeholders
                )
            else:
                non_key_columns = [col for col in df.columns if col not in primary_keys]
                updates = sql.SQL(', ').join([
                    sql.Identifier(col) + sql.SQL(" = EXCLUDED.") + sql.Identifier(col)
                    for col in non_key_columns
                ])
                
                insert_sql = sql.SQL("""
                    INSERT INTO {table} ({columns})
                    VALUES ({values})
                    ON CONFLICT ({pkey}) DO UPDATE SET
                        {updates},
                        updated_at = NOW()
                """).format(
                    table=sql.Identifier(table_name),
                    columns=columns,
                    values=placeholders,
                    pkey=sql.SQL(', ').join(map(sql.Identifier, primary_keys)),
                    updates=updates
                )

            # 批量写入数据
            with conn.cursor() as cur:
                records = df.to_dict('records')
                # 确保每条记录是元组形式
                values = [tuple(record.values()) for record in records]
                execute_batch(cur, insert_sql, values)
            conn.commit()
            
            logger.info(f"成功写入{table_name} {len(df)}条记录")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"数据写入失败: {str(e)}")
            raise DatabaseError(f"数据写入失败: {str(e)}")
        finally:
            self._return_connection(conn)

    def __del__(self):
        """关闭连接池"""
        if hasattr(self, 'pool'):
            self.pool.closeall()

# 单例模式全局访问
storage = QuantStorage()