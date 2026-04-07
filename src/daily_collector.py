#!/usr/bin/env python3
"""
每日增量采集脚本 - 5分钟K线
使用 baostock 获取A股5分钟K线数据

使用方法:
    python daily_collector.py                    # 增量采集
    python daily_collector.py --force-today     # 强制采集今天的数据
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import clickhouse_connect
import baostock as bs
import pandas as pd
import yaml
import logging
from datetime import datetime, timedelta, date
from tqdm import tqdm
import time
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('daily_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DailyCollector:
    """每日增量采集器 - baostock 5分钟K线"""
    
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
        result = self.client.query('''
            SELECT DISTINCT symbol FROM stock_data.stock_minute ORDER BY symbol
        ''')
        
        stocks = []
        for row in result.result_rows:
            symbol = row[0]
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
                adjustflag='2'
            )
            
            if rs.error_code != '0':
                return None
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return None
            
            return pd.DataFrame(data_list, columns=rs.fields)
            
        except Exception:
            return None
    
    def parse_time(self, row):
        """解析 baostock 的 time 字段"""
        time_str = row['time']
        year = int(time_str[0:4])
        month = int(time_str[4:6])
        day = int(time_str[6:8])
        hour = int(time_str[8:10])
        minute = int(time_str[10:12])
        return datetime(year, month, day, hour, minute)
    
    def insert_data(self, df, symbol_db):
        """批量插入数据"""
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
                    symbol_db,
                    trade_date,
                    trade_time,
                    trade_time,
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(float(row['volume'])),
                    0.0,
                    datetime.now()
                ])
            except Exception:
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
            logger.debug(f"插入失败: {e}")
            return 0
    
    def run(self, target_date=None):
        """执行每日采集"""
        start_time = datetime.now()
        
        # 确定采集日期
        if not target_date:
            last_date = self.get_last_date()
            if last_date:
                target_date = last_date + timedelta(days=1)
            else:
                logger.error("无法确定采集日期")
                return None
        
        target_str = target_date.strftime('%Y-%m-%d') if isinstance(target_date, date) else target_date
        
        # 检查是否是交易日（简单检查：周六周日跳过）
        if isinstance(target_date, date):
            if target_date.weekday() >= 5:
                logger.info(f"{target_date} 是周末，跳过")
                return None
        
        logger.info(f"开始采集: {target_str}")
        
        stocks = self.get_all_stocks()
        total_inserted = 0
        success_count = 0
        
        logger.info(f"开始遍历 {len(stocks)} 只股票...")
        
        for symbol_baostock in tqdm(stocks, desc="采集进度"):
            try:
                df = self.fetch_5min_data(symbol_baostock, target_str, target_str)
                
                if df is not None and not df.empty:
                    if symbol_baostock.startswith('sh.'):
                        symbol_db = symbol_baostock[3:] + '.SH'
                    else:
                        symbol_db = symbol_baostock[3:] + '.SZ'
                    
                    inserted = self.insert_data(df, symbol_db)
                    total_inserted += inserted
                    if inserted > 0:
                        success_count += 1
                
                time.sleep(0.05)
                
            except Exception as e:
                logger.debug(f"{symbol_baostock} 处理失败: {e}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"采集完成!")
        logger.info(f"日期: {target_str}")
        logger.info(f"成功: {success_count} 只股票")
        logger.info(f"新增记录: {total_inserted}")
        logger.info(f"耗时: {elapsed/60:.1f}分钟")
        
        return {
            'date': target_str,
            'success': success_count,
            'inserted': total_inserted,
            'elapsed': elapsed
        }
    
    def close(self):
        self.bs.logout()
        self.client.close()


def main():
    parser = argparse.ArgumentParser(description='每日增量采集5分钟K线数据')
    parser.add_argument('--date', type=str, help='目标日期 YYYY-MM-DD（默认昨天）')
    parser.add_argument('--force-today', action='store_true', help='强制采集今天的数据')
    parser.add_argument('--config', default='config.yaml', help='配置文件')
    args = parser.parse_args()
    
    collector = DailyCollector(args.config)
    
    if args.force_today:
        target_date = date.today()
    elif args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        # 默认昨天
        target_date = date.today() - timedelta(days=1)
    
    result = collector.run(target_date=target_date)
    collector.close()
    
    if result:
        print(f"\n采集结果: {result}")


if __name__ == '__main__':
    main()
