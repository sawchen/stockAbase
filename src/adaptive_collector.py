#!/usr/bin/env python3
"""
自适应增量采集脚本 - baostock 5分钟K线
自动检测限流并调整请求间隔
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
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('adaptive_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AdaptiveCollector:
    """自适应采集器 - 自动调整请求间隔"""
    
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
        
        # 自适应参数 - 初始值更大，避免触发限流
        self.base_interval = 2.0  # 基础间隔（秒）
        self.current_interval = 2.0
        self.min_interval = 0.5
        self.max_interval = 10.0
        self.consecutive_errors = 0
        self.total_requests = 0
        self.rate_limit_detected = False
        
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
    
    def increase_interval(self):
        """增加请求间隔（检测到限流时）"""
        old_interval = self.current_interval
        self.current_interval = min(self.current_interval * 1.5, self.max_interval)
        self.consecutive_errors += 1
        self.rate_limit_detected = True
        logger.warning(f"检测到限流! 间隔: {old_interval:.2f}s -> {self.current_interval:.2f}s (连续错误: {self.consecutive_errors})")
    
    def decrease_interval(self):
        """减少请求间隔（连续成功时）"""
        if self.consecutive_errors > 0:
            self.consecutive_errors = 0
            return
        
        old_interval = self.current_interval
        self.current_interval = max(self.current_interval * 0.95, self.base_interval)
        if old_interval != self.current_interval:
            logger.info(f"性能优化: 间隔 {old_interval:.2f}s -> {self.current_interval:.2f}s")
    
    def fetch_5min_data(self, symbol_baostock, start_date, end_date):
        """获取单只股票5分钟数据，带自适应间隔"""
        try:
            rs = self.bs.query_history_k_data_plus(
                symbol_baostock,
                'date,time,open,high,low,close,volume',
                start_date=start_date,
                end_date=end_date,
                frequency='5',
                adjustflag='2'
            )
            
            self.total_requests += 1
            
            # 检查错误
            if rs.error_code != '0':
                self.increase_interval()
                return None
            
            # 成功，连续成功可以降低间隔
            self.decrease_interval()
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return None
            
            return pd.DataFrame(data_list, columns=rs.fields)
            
        except Exception as e:
            self.increase_interval()
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
            logger.warning(f"插入失败: {e}")
            return 0
    
    def run(self, start_date=None, end_date=None):
        """执行采集"""
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
        logger.info(f"初始请求间隔: {self.current_interval}s")
        
        stocks = self.get_all_stocks()
        total_stocks = len(stocks)
        total_inserted = 0
        success_count = 0
        
        logger.info(f"开始遍历 {total_stocks} 只股票...")
        
        for i, symbol_baostock in enumerate(stocks):
            try:
                df = self.fetch_5min_data(symbol_baostock, start_date, end_date)
                
                if df is not None and not df.empty:
                    if symbol_baostock.startswith('sh.'):
                        symbol_db = symbol_baostock[3:] + '.SH'
                    else:
                        symbol_db = symbol_baostock[3:] + '.SZ'
                    
                    inserted = self.insert_data(df, symbol_db)
                    total_inserted += inserted
                    if inserted > 0:
                        success_count += 1
                
                # 自适应间隔
                time.sleep(self.current_interval)
                
                # 每100只股票输出进度
                if (i + 1) % 100 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = (i + 1) / elapsed * 60  # 每分钟处理数
                    eta = (total_stocks - i - 1) / rate if rate > 0 else 0
                    logger.info(f"进度: {i+1}/{total_stocks} ({100*(i+1)/total_stocks:.1f}%) | "
                               f"速率: {rate:.1f}/min | 当前间隔: {self.current_interval:.2f}s | "
                               f"剩余: {eta:.0f}分钟 | 已插入: {total_inserted}")
                    
                    # 检测到限流时暂停更久
                    if self.rate_limit_detected:
                        logger.warning("检测到限流，暂停10秒...")
                        time.sleep(10)
                        self.rate_limit_detected = False
                
            except Exception as e:
                logger.debug(f"{symbol_baostock} 处理失败: {e}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"采集完成!")
        logger.info(f"日期范围: {start_date} ~ {end_date}")
        logger.info(f"处理股票: {total_stocks}")
        logger.info(f"成功: {success_count}")
        logger.info(f"新增记录: {total_inserted:,}")
        logger.info(f"耗时: {elapsed/60:.1f}分钟")
        logger.info(f"总请求数: {self.total_requests}")
        logger.info(f"最终间隔: {self.current_interval:.2f}s")
        
        return {
            'start_date': start_date,
            'end_date': end_date,
            'total_stocks': total_stocks,
            'success': success_count,
            'inserted': total_inserted,
            'elapsed_seconds': elapsed,
            'total_requests': self.total_requests,
            'final_interval': self.current_interval
        }
    
    def close(self):
        self.bs.logout()
        self.client.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='自适应采集5分钟K线数据')
    parser.add_argument('--start', type=str, help='开始日期 YYYY-MM-DD')
    parser.add_argument('--end', type=str, help='结束日期 YYYY-MM-DD')
    parser.add_argument('--config', default='config.yaml', help='配置文件')
    args = parser.parse_args()
    
    collector = AdaptiveCollector(args.config)
    
    result = collector.run(
        start_date=args.start,
        end_date=args.end
    )
    
    collector.close()
    
    print(f"\n采集结果: {result}")


if __name__ == '__main__':
    main()
