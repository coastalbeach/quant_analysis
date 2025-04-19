"""PostgreSQL数据存储抽象层 (psycopg3版本)
功能特性：
1. 自动表结构推断与动态建表
2. 数据去重与增量更新机制
3. 支持多种频率数据的统一处理
4. 事务管理与批量写入优化
5. 支持自定义主键和表结构变更
6. 利用psycopg3高性能连接池和异步特性
"""

import logging
from typing import Dict, Any, List, Optional, Union
import pandas as pd
import yaml
import psycopg
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier, Literal, Composed
from psycopg_pool import ConnectionPool

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
                    'min_size': config.get('pool', {}).get('minconn', 5),  # 增加最小连接数
                    'max_size': config.get('pool', {}).get('maxconn', 50)  # 显著增加最大连接数
                }
        except Exception as e:
            raise DatabaseError(f"配置加载失败: {str(e)}")

    def _init_connection_pool(self) -> None:
        """初始化数据库连接池
        
        psycopg3使用新的连接池API，支持异步操作和更好的连接管理
        增加了连接池超时和重试配置，以及连接健康检查
        """
        try:
            # 构建连接字符串
            conninfo = f"host={self.db_config['host']} "
            conninfo += f"port={self.db_config['port']} "
            conninfo += f"dbname={self.db_config['database']} "
            conninfo += f"user={self.db_config['user']} "
            conninfo += f"password={self.db_config['password']}"
            
            # 创建连接池 - 优化连接池配置
            self.pool = ConnectionPool(
                conninfo=conninfo,
                min_size=self.pool_config['min_size'],
                max_size=self.pool_config['max_size'],
                # 设置连接工厂，使查询结果默认返回字典
                kwargs={
                    "row_factory": dict_row,
                    "autocommit": False,  # 显式控制事务
                    "connect_timeout": 30  # 增加连接超时时间
                },
                # 连接池配置
                timeout=10,  # 增加获取连接的超时时间
                max_waiting=100,  # 增加最大等待连接数
                max_lifetime=1800,  # 减少连接最大生命周期以更频繁地刷新连接
                num_workers=4  # 增加后台工作线程数
            )
            
            # 测试连接并进行健康检查
            conn = self.pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                conn.commit()
                logger.info("数据库连接池初始化成功")
            finally:
                self.pool.putconn(conn)
                
        except Exception as e:
            logger.error(f"连接池初始化失败: {str(e)}")
            raise DatabaseError(f"连接池初始化失败: {str(e)}")

    def _get_connection(self):
        """获取数据库连接，使用增强的重试机制和SSL错误处理"""
        max_retries = 5  # 增加最大重试次数
        retry_count = 0
        base_wait = 2  # 增加基础等待时间
        
        while retry_count < max_retries:
            try:
                conn = self.pool.getconn()
                # 增强的连接健康检查
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                    # 检查事务状态
                    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                        conn.rollback()
                return conn
            except psycopg.errors.OperationalError as e:
                # 处理SSL和网络相关错误
                if "SSL" in str(e) or "timeout" in str(e):
                    retry_count += 1
                    if retry_count == max_retries:
                        logger.error(f"SSL/网络连接失败(重试{max_retries}次): {str(e)}")
                        raise DatabaseError(f"SSL/网络连接失败: {str(e)}")
                    
                    # 使用更长的等待时间
                    wait_time = base_wait * (2 ** (retry_count - 1))  # 2, 4, 8, 16, 32秒
                    logger.warning(f"SSL/网络连接失败，{wait_time}秒后重试({retry_count}/{max_retries}): {str(e)}")
                    
                    import time
                    time.sleep(wait_time)
                else:
                    # 其他操作错误直接抛出
                    raise DatabaseError(f"数据库连接失败: {str(e)}")
            except psycopg.Error as e:
                retry_count += 1
                if retry_count == max_retries:
                    logger.error(f"获取数据库连接失败(重试{max_retries}次): {str(e)}")
                    raise DatabaseError(f"获取数据库连接失败: {str(e)}")
                
                # 计算指数退避等待时间
                wait_time = base_wait * (2 ** (retry_count - 1))
                logger.warning(f"获取连接失败，{wait_time}秒后重试({retry_count}/{max_retries}): {str(e)}")
                
                import time
                time.sleep(wait_time)

    def _return_connection(self, conn) -> None:
        """归还连接到连接池，确保连接状态正确"""
        try:
            # 如果连接有未提交的事务，进行回滚
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                conn.rollback()
            self.pool.putconn(conn)
        except Exception as e:
            logger.warning(f"归还连接到连接池失败: {str(e)}")
            # 如果归还失败，尝试关闭连接
            try:
                conn.close()
            except:
                pass
        
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
                # 使用参数化查询，避免SQL注入
                placeholders = ",".join([f"%s" for _ in stock_codes])
                query = f"SELECT 代码, 市场 FROM 基础 WHERE 代码 IN ({placeholders})"
                cur.execute(query, stock_codes)
                results = cur.fetchall()
                market_map = {row["代码"]: row["市场"] for row in results}
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
        query = SQL("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
        """)
        
        with conn.cursor() as cur:
            cur.execute(query, [table_name])
            return {row["column_name"]: row["data_type"] for row in cur.fetchall()}

    def _alter_table(self, table_name: str, new_columns: Dict[str, str], conn) -> None:
        """添加新列到现有表"""
        for col, dtype in new_columns.items():
            alter_query = SQL("""
                ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {type}
            """).format(
                table=Identifier(table_name),
                column=Identifier(col),
                type=SQL(dtype)
            )
            with conn.cursor() as cur:
                cur.execute(alter_query)

    def _create_table(self, table_name: str, schema: Dict[str, str], 
                      primary_keys: List[str], conn, partition_by: Optional[str] = None) -> None:
        """动态创建数据表，支持PostgreSQL 16原生分区
        
        支持两种分区策略：
        1. 行情数据：父表按市场分区，子表按频率分区
        2. 其他数据：按指定字段分区（如年月）
        
        注意：分区表的主键必须包含所有分区列
        """
        columns = [Identifier(col) + SQL(" " + dtype)
                  for col, dtype in schema.items()]
        
        # 添加系统字段
        columns.extend([
            SQL("created_at TIMESTAMP DEFAULT NOW()"),
            SQL("updated_at TIMESTAMP DEFAULT NOW()")
        ])
        
        # 检查分区类型
        is_market_freq_partition = '市场' in schema and '频率' in schema
        is_custom_partition = partition_by is not None and partition_by in schema
        
        # 检查是否为股票行情表（用于日志和其他特殊处理）
        is_quote_table = table_name.startswith('股票行情_前复权') or table_name.startswith('股票行情_后复权')
        
        # 处理分区表
        if is_market_freq_partition or is_custom_partition:
            # 确保主键包含分区列
            if is_market_freq_partition:
                partition_keys = ['市场', '频率']
                partition_column = '"市场"'
            else:  # 自定义分区
                partition_keys = [partition_by]
                partition_column = f'"{partition_by}"'
            
            # 将分区键添加到主键中
            for key in partition_keys:
                if key not in primary_keys:
                    primary_keys.append(key)
            
            query = SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {columns},
                    PRIMARY KEY ({pkey})
                ) PARTITION BY LIST ({partition_column});
            """).format(
                table=Identifier(table_name),
                columns=SQL(',\n').join(columns),
                pkey=SQL(', ').join(map(Identifier, primary_keys)),
                partition_column=SQL(partition_column)
            )
            
            with conn.cursor() as cur:
                cur.execute(query)
        else:
            # 对于非分区表，创建普通表
            query = SQL("""
                CREATE TABLE IF NOT EXISTS {table} (
                    {columns},
                    PRIMARY KEY ({pkey})
                )
            """).format(
                table=Identifier(table_name),
                columns=SQL(',\n').join(columns),
                pkey=SQL(', ').join(map(Identifier, primary_keys))
            )
            
            with conn.cursor() as cur:
                cur.execute(query)

    def _ensure_partition_exists(self, table_name: str, conn, **kwargs) -> None:
        """确保分区表存在，如果不存在则创建
        
        支持两种分区模式：
        1. 市场+频率分区：需要提供market和frequency参数
        2. 自定义分区：需要提供partition_field和partition_value参数
        
        Args:
            table_name: 父表名称
            conn: 数据库连接
            **kwargs: 其他参数，包括：
                - market, frequency: 市场和频率分区
                - partition_field, partition_value: 自定义分区字段和值
        """
        # 确定分区模式
        is_market_freq_mode = 'market' in kwargs and 'frequency' in kwargs
        is_custom_mode = 'partition_field' in kwargs and 'partition_value' in kwargs
        
        if not (is_market_freq_mode or is_custom_mode):
            raise ValueError("必须提供市场+频率分区或自定义分区的参数")
        
        # 生成分区表名
        if is_market_freq_mode:
            market = kwargs['market']
            frequency = kwargs['frequency']
            partition_name = f"{table_name}_{market}_{frequency}"
            parent_partition = f"{table_name}_{market}"
        else:
            partition_field = kwargs['partition_field']
            partition_value = kwargs['partition_value']
            partition_name = f"{table_name}_{partition_value}"
            parent_partition = None  # 自定义分区模式下没有父分区
        
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
                if is_market_freq_mode:
                    # 先检查父分区是否存在
                    cur.execute(check_query, (parent_partition,))
                    parent_exists = cur.fetchone() is not None
                    
                    if not parent_exists:
                        # 创建父分区表
                        create_partition_query = SQL("""
                            CREATE TABLE IF NOT EXISTS {partition} PARTITION OF {parent}
                            FOR VALUES IN ({market_value})
                            PARTITION BY LIST ("频率");
                        """).format(
                            partition=Identifier(parent_partition),
                            parent=Identifier(table_name),
                            market_value=Literal(market)
                        )
                        cur.execute(create_partition_query)
                    
                    # 创建子分区
                    create_subpartition_query = SQL("""
                        CREATE TABLE IF NOT EXISTS {subpartition} PARTITION OF {partition}
                        FOR VALUES IN ({frequency_value});
                    """).format(
                        subpartition=Identifier(partition_name),
                        partition=Identifier(parent_partition),
                        frequency_value=Literal(frequency)
                    )
                    cur.execute(create_subpartition_query)
                else:  # 自定义分区模式
                    # 直接创建分区
                    create_partition_query = SQL("""
                        CREATE TABLE IF NOT EXISTS {partition} PARTITION OF {parent}
                        FOR VALUES IN ({partition_value});
                    """).format(
                        partition=Identifier(partition_name),
                        parent=Identifier(table_name),
                        partition_value=Literal(partition_value)
                    )
                    cur.execute(create_partition_query)
                
                logger.info(f"创建分区表: {partition_name}")

    def save(self,
             df: pd.DataFrame,
             data_type: str,
             frequency: str = None,
             primary_keys: Optional[List[str]] = None,
             replace: bool = False,
             adjust_type: str = None,
             partition_by: Optional[str] = None,
             upsert_fields: Optional[List[str]] = None) -> None:
        """主存储方法
        :param df: 待存储数据框
        :param data_type: 数据类型（基础, 行情, 行业板块, 概念板块...）
        :param frequency: 数据频率（daily, weekly, monthly）
        :param primary_keys: 主键字段列表，默认为['代码', '日期']
        :param replace: 是否全量替换
        :param adjust_type: 复权类型（前复权, 后复权）
        :param partition_by: 分区字段，用于按指定字段分区
        :param upsert_fields: 冲突时需要更新的字段列表，如果为None则更新所有非主键字段
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
            # 开始事务
            conn.autocommit = False
            
            # 检查表是否存在，不存在则创建
            self._create_table(table_name, schema, primary_keys, conn, partition_by=partition_by)
            
            # 检查表结构变更
            existing_columns = self._get_existing_columns(table_name, conn)
            new_columns = {k: v for k, v in schema.items() if k not in existing_columns}
            if new_columns:
                self._alter_table(table_name, new_columns, conn)
            
            # 处理分区表
            is_quote_table = table_name.startswith('股票行情_前复权') or table_name.startswith('股票行情_后复权')
            
            # 市场+频率分区（行情数据）
            if is_quote_table and '市场' in df.columns and '频率' in df.columns:
                # 按市场和频率分组处理数据
                for (market, freq), group_df in df.groupby(['市场', '频率']):
                    if not group_df.empty:
                        # 确保分区存在
                        self._ensure_partition_exists(table_name, conn, market=market, frequency=freq)
            
            # 自定义分区（如按年月分区）
            elif partition_by and partition_by in df.columns:
                # 按分区字段分组处理数据
                for partition_value, group_df in df.groupby(partition_by):
                    if not group_df.empty:
                        # 确保分区存在
                        self._ensure_partition_exists(table_name, conn, 
                                                     partition_field=partition_by, 
                                                     partition_value=partition_value)
            
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
            columns = SQL(', ').join(map(Identifier, df.columns))
            # psycopg3中使用%s作为参数占位符
            placeholders = SQL(', ').join([SQL('%s')] * len(df.columns))
            
            if replace:
                insert_sql = SQL("""
                    INSERT INTO {table} ({columns})
                    VALUES ({values})
                """).format(
                    table=Identifier(table_name),
                    columns=columns,
                    values=placeholders
                )
            else:
                # 确定需要更新的字段
                if upsert_fields:
                    update_columns = [col for col in upsert_fields if col in df.columns and col not in primary_keys]
                else:
                    update_columns = [col for col in df.columns if col not in primary_keys]
                
                # 检查是否有需要更新的字段
                if update_columns:
                    updates = SQL(', ').join([
                        Identifier(col) + SQL(" = EXCLUDED.") + Identifier(col)
                        for col in update_columns
                    ])
                    
                    insert_sql = SQL("""
                        INSERT INTO {table} ({columns})
                        VALUES ({values})
                        ON CONFLICT ({pkey}) DO UPDATE SET
                            {updates},
                            updated_at = NOW()
                    """).format(
                        table=Identifier(table_name),
                        columns=columns,
                        values=placeholders,
                        pkey=SQL(', ').join(map(Identifier, primary_keys)),
                        updates=updates
                    )
                else:
                    # 如果没有需要更新的字段，则只更新updated_at
                    insert_sql = SQL("""
                        INSERT INTO {table} ({columns})
                        VALUES ({values})
                        ON CONFLICT ({pkey}) DO UPDATE SET
                            updated_at = NOW()
                    """).format(
                        table=Identifier(table_name),
                        columns=columns,
                        values=placeholders,
                        pkey=SQL(', ').join(map(Identifier, primary_keys))
                    )

            # 批量写入数据 - psycopg3的批量操作
            with conn.cursor() as cur:
                records = df.to_dict('records')
                # 确保每条记录是元组形式
                values = [tuple(record.values()) for record in records]
                # 使用psycopg3的批量插入方法
                cur.executemany(insert_sql, values)
                
            conn.commit()
            
            #logger.info(f"成功写入{table_name} {len(df)}条记录")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"数据写入失败: {str(e)}")
            raise DatabaseError(f"数据写入失败: {str(e)}")
        finally:
            self._return_connection(conn)

    def __del__(self):
        """关闭连接池"""
        if hasattr(self, 'pool'):
            self.pool.close()

# 单例模式全局访问
storage = QuantStorage()