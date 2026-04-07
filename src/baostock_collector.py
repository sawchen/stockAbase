#!/usr/bin/env python3
"""
baostock 数据采集脚本 - 5分钟K线
支持增量补数和每日采集
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import clickhouse_connect
import baostock as bs
import pandas as pd
import yaml
import logging
from datetime import datetime, timedelta
from tqdm import tqdm
import time
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('baostock_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BaostockCollector:
    """baostock 5分钟K线采集器"""
    
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        db_conf = self.config['clickhouse']
        self.client = clickhouse_connect.get_client(
            host=db_conf['host'],
            port=db_conf['port'],
            user=db_conf['user'],
            password=db_conf['password'],
            database='stock_data'
        )
        
        self.bs = bs
        self.bs.login()
        logger.info("baostock 登录成功")
    
    def get_all_stocks(self):
        """获取所有A股股票列表"""
        logger.info("获取A股股票列表...")
        result = self.client.query('''
            SELECT DISTINCT symbol FROM stock_data.stock_minute ORDER BY symbol
        ''')
        
        stocks = []
        for row in result.result_rows:
            symbol = row[0]
            # 转换为 baostock 格式: 000001.SZ -> sz.000001, 600000.SH -> sh.600000
            if symbol.endswith('.SH'):
                stocks.append('sh.' + symbol.split('.')[0])
            elif symbol.endswith('.SZ'):
                stocks.append('sz.' + symbol.split('.')[0])
        
        logger.info(f"获取到 {len(stocks)} 只股票")
        return stocks
    
    def get_last_date(self):
        """获取数据库中最后的交易日期"""
        result = self.client.query('SELECT max(trade_date) FROM stock_data.stock_minute')
        last_date = result.result_rows[0][0]
        logger.info(f"数据库最后交易日期: {last_date}")
        return last_date
    
    def fetch_5min_data(self, symbol_baostock, start_date, end_date):
        """获取单只股票5分钟数据"""
        try:
            rs = self.bs.query_history_k_data_plus(
                symbol_baostock,
                'date,time,open,high,low,close,volume',
                start_date=start_date,
                end_date=end_date,
                frequency='5',
                adjustflag='2'  # 前复权
            )
            
            if rs.error_code != '0':
                logger.warning(f"{symbol_baostock} 查询失败: {rs.error_msg}")
                return None
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return None
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            return df
            
        except Exception as e:
            logger.debug(f"{symbol_baostock} 异常: {e}")
            return None
    
    def parse_time(self, row):
        """解析 baostock 的 time 字段 (格式: 20260224093500)"""
        date_str = row['date']  # 2026-02-24
        time_str = row['time']   # 20260224093500
        
        # time_str 格式: YYYYMMDDHHMMSS
        year = int(time_str[0:4])
        month = int(time_str[4:6])
        day = int(time_str[6:8])
        hour = int(time_str[8:10])
        minute = int(time_str[10:12])
        
        from datetime import datetime
        return datetime(year, month, day, hour, minute)
    
    def insert_data(self, df, symbol_db):
        """批量插入数据到ClickHouse"""
        if df is None or df.empty:
            return 0
        
        rows = []
        trade_date = None
        
        for _, row in df.iterrows():
            try:
                trade_time = self.parse_time(row)
                if trade_date is None:
                    trade_date = trade_time.date()
                
                rows.append([
                    symbol_db,                    # symbol
                    trade_date,                   # trade_date
                    trade_time,                   # trade_time
                    trade_time,                   # trade_time_cst
                    float(row['open']),            # open
                    float(row['high']),            # high
                    float(row['low']),             # low
                    float(row['close']),           # close
                    int(float(row['volume'])),     # volume
                    0.0,                          # amount (baostock无此字段)
                    datetime.now()                # created_at
                ])
            except Exception as e:
                logger.debug(f"数据解析错误: {row}, {e}")
                continue
        
        if not rows:
            return 0
        
        column_names = [
            'symbol', 'trade_date', 'trade_time', 'trade_time_cst',
            'open', 'high', 'low', 'close', 'volume', 'amount', 'created_at'
        ]
        
        try:
            self.client.insert('stock_data.stock_minute', rows, column_names=column_names)
            return len(rows)
        except Exception as e:
            logger.warning(f"插入失败: {e}")
            return 0
    
    def collect(self, symbols=None, start_date=None, end_date=None, batch_size=50,
                symbols_file=None):
        """
        执行采集
        
        Args:
            symbols: 股票列表，默认全部
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            batch_size: 每批股票数（控制频率避免限流）
            symbols_file: 从文件读取股票列表（一行一个）
        """
        # 从文件读取股票列表
        if symbols_file:
            with open(symbols_file, 'r') as f:
                symbols = [line.strip() for line in f if line.strip()]
            logger.info(f"从文件 {symbols_file} 读取了 {len(symbols)} 只股票")
        start_time = datetime.now()
        
        # 确定日期范围
        if not end_date:
            end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        if not start_date:
            last_date = self.get_last_date()
            if last_date:
                start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                start_date = '2026-02-01'
        
        logger.info(f"采集范围: {start_date} ~ {end_date}")
        
        # 获取股票列表
        if not symbols:
            symbols = self.get_all_stocks()
        
        total_inserted = 0
        total_stocks = len(symbols)
        
        logger.info(f"开始遍历 {total_stocks} 只股票...")
        
        for i in range(0, total_stocks, batch_size):
            batch = symbols[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_stocks + batch_size - 1) // batch_size
            
            logger.info(f"处理批次 {batch_num}/{total_batches} ({len(batch)} 只股票)")
            
            for symbol_baostock in tqdm(batch, desc=f"批次{batch_num}"):
                try:
                    df = self.fetch_5min_data(symbol_baostock, start_date, end_date)
                    
                    if df is not None and not df.empty:
                        # 转换股票代码格式
                        if symbol_baostock.startswith('sh.'):
                            symbol_db = symbol_baostock[3:] + '.SH'
                        else:
                            symbol_db = symbol_baostock[3:] + '.SZ'
                        
                        inserted = self.insert_data(df, symbol_db)
                        total_inserted += inserted
                    
                    # 避免请求过快
                    time.sleep(0.05)
                    
                except Exception as e:
                    logger.debug(f"{symbol_baostock} 处理失败: {e}")
            
            # 批次间暂停，避免限流
            if i + batch_size < total_stocks:
                logger.info(f"批次完成，暂停3秒...")
                time.sleep(3)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"采集完成!")
        logger.info(f"日期范围: {start_date} ~ {end_date}")
        logger.info(f"处理股票: {total_stocks}")
        logger.info(f"新增记录: {total_inserted:,}")
        logger.info(f"耗时: {elapsed/60:.1f}分钟")
        
        return {
            'start_date': start_date,
            'end_date': end_date,
            'total_stocks': total_stocks,
            'inserted': total_inserted,
            'elapsed_seconds': elapsed
        }
    
    def close(self):
        self.bs.logout()
        self.client.close()


def main():
    parser = argparse.ArgumentParser(description='baostock 5分钟K线采集')
    parser.add_argument('--start', type=str, help='开始日期 YYYY-MM-DD')
    parser.add_argument('--end', type=str, help='结束日期 YYYY-MM-DD')
    parser.add_argument('--symbols', nargs='+', help='指定股票代码(baostock格式)')
    parser.add_argument('--symbols-file', type=str, help='从文件读取股票列表(一行一个)')
    parser.add_argument('--batch-size', type=int, default=50, help='每批股票数')
    parser.add_argument('--config', default='config.yaml', help='配置文件')
    args = parser.parse_args()
    
    collector = BaostockCollector(args.config)
    
    result = collector.collect(
        symbols=args.symbols,
        start_date=args.start,
        end_date=args.end,
        batch_size=args.batch_size,
        symbols_file=args.symbols_file
    )
    
    collector.close()
    
    print(f"\n采集结果: {result}")


if __name__ == '__main__':
    main()
