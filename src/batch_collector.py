#!/usr/bin/env python3
"""
断点续传批次采集器 - baostock 5分钟K线
每批100只股票，处理完自动停止，支持重启继续
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
import atexit

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('batch_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BatchCollector:
    """断点续传批次采集器"""
    
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
        
        # 进度文件
        self.progress_file = 'batch_progress.json'
        self.batch_size = 100
        self.inter_batch_pause = 60  # 每批次完成后休息60秒
        self.rate_limit_pause = 60  # 限流时休息60秒
        
        # 加载进度
        self.progress = self.load_progress()
        
        atexit.register(self.save_progress)
    
    def load_progress(self):
        """加载进度"""
        if os.path.exists(self.progress_file):
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {
            'all_stocks': [],      # 全部股票列表
            'completed_stocks': [], # 已完成的股票
            'last_index': 0,       # 当前批次起始索引
            'total_inserted': 0,   # 总插入记录数
            'batches_completed': 0  # 完成的批次数
        }
    
    def save_progress(self):
        """保存进度"""
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
        logger.info(f"进度已保存: 已完成 {len(self.progress['completed_stocks'])} 只股票")
    
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
        time_str = row['time']
        return datetime(
            int(time_str[0:4]), int(time_str[4:6]), int(time_str[6:8]),
            int(time_str[8:10]), int(time_str[10:12])
        )
    
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
                    symbol_db, trade_date, trade_time, trade_time,
                    float(row['open']), float(row['high']), float(row['low']),
                    float(row['close']), int(float(row['volume'])), 0.0, datetime.now()
                ])
            except Exception:
                continue
        
        if not rows:
            return 0
        
        try:
            self.client.insert('stock_data.stock_minute', rows,
                column_names=['symbol','trade_date','trade_time','trade_time_cst',
                             'open','high','low','close','volume','amount','created_at'])
            return len(rows)
        except Exception:
            return 0
    
    def run_batch(self, start_date, end_date):
        """运行一批采集"""
        bs.login()
        logger.info("baostock 登录成功")
        
        # 获取/更新股票列表
        if not self.progress['all_stocks']:
            self.progress['all_stocks'] = self.get_all_stocks()
        
        all_stocks = self.progress['all_stocks']
        completed = set(self.progress['completed_stocks'])
        
        # 找出未完成的股票，过滤掉"complete"股票(DB已有最新数据)
        target_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        remaining = []
        skipped_complete = 0
        
        for s in all_stocks:
            if s in completed:
                continue
            
            # 转换格式检查DB
            if s.endswith('.SH'):
                symbol_db = s[3:] + '.SH'
            else:
                symbol_db = s[3:] + '.SZ'
            
            r = self.client.query(f"SELECT max(trade_date) FROM stock_data.stock_minute WHERE symbol = '{symbol_db}'")
            last_db_date = r.result_rows[0][0] if r.result_rows and r.result_rows[0][0] else None
            
            # 如果DB已有最新数据，跳过
            if last_db_date and last_db_date >= target_date - timedelta(days=1):
                skipped_complete += 1
                completed.add(s)  # 标记为已跳过
                continue
            
            remaining.append(s)
        
        if skipped_complete > 0:
            logger.info(f"跳过 {skipped_complete} 只已完成股票(DB已有最新数据)")
        
        logger.info(f"待处理: {len(remaining)} 只股票")
        
        if not remaining:
            logger.info("全部股票已处理完成!")
            bs.logout()
            return True
        
        # 处理一批
        batch = remaining[:self.batch_size]
        batch_num = self.progress['batches_completed'] + 1
        
        logger.info(f"=== 批次 {batch_num} 开始 ===")
        logger.info(f"处理股票: {len(batch)} 只")
        logger.info(f"起始索引: {self.progress['last_index']}")
        
        inserted_this_batch = 0
        success_this_batch = 0
        failed_this_batch = 0
        
        for i, symbol_baostock in enumerate(batch):
            try:
                # 转换格式
                if symbol_baostock.startswith('sh.'):
                    symbol_db = symbol_baostock[3:] + '.SH'
                else:
                    symbol_db = symbol_baostock[3:] + '.SZ'
                
                # 先检查数据库中该股票最新日期
                r = self.client.query(f"SELECT max(trade_date) FROM stock_data.stock_minute WHERE symbol = '{symbol_db}'")
                last_db_date = r.result_rows[0][0] if r.result_rows and r.result_rows[0][0] else None
                
                # 如果数据库里最新日期已经>=目标结束日期-1天，跳过（说明已有最新数据）
                target_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                if last_db_date and last_db_date >= target_date - timedelta(days=1):
                    logger.info(f"  {symbol_baostock}: DB已有最新数据 {last_db_date}，跳过")
                    self.progress['completed_stocks'].append(symbol_baostock)
                    continue
                
                # 获取baostock数据 - 只获取DB最新日期之后的数据
                fetch_start = (last_db_date + timedelta(days=1)).strftime('%Y-%m-%d') if last_db_date else start_date
                df = self.fetch_5min_data(symbol_baostock, fetch_start, end_date)
                
                inserted = 0
                if df is not None and not df.empty:
                    inserted = self.insert_data(df, symbol_db)
                    inserted_this_batch += inserted
                    if inserted > 0:
                        success_this_batch += 1
                        logger.info(f"  {symbol_baostock}: 插入 {inserted} 条")
                    else:
                        # 插入了0条，可能是重复数据
                        logger.info(f"  {symbol_baostock}: 无新数据")
                
                # 标记为已处理
                # 如果是"complete"股票（DB已有最新数据），也标记为已完成
                target_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                if inserted > 0 or (last_db_date and last_db_date >= target_date - timedelta(days=1)):
                    self.progress['completed_stocks'].append(symbol_baostock)
                
                # 间隔
                time.sleep(0.5)
                
                # 进度输出
                if (i + 1) % 10 == 0:
                    logger.info(f"  进度: {i+1}/{len(batch)} ({100*(i+1)/len(batch):.0f}%)")
                
            except Exception as e:
                failed_this_batch += 1
                logger.warning(f"  {symbol_baostock} 失败: {e}")
        
        bs.logout()
        
        # 更新进度
        self.progress['batches_completed'] += 1
        self.progress['total_inserted'] += inserted_this_batch
        self.save_progress()
        
        logger.info(f"=== 批次 {batch_num} 完成 ===")
        logger.info(f"成功: {success_this_batch} 只")
        logger.info(f"失败: {failed_this_batch} 只")
        logger.info(f"本批插入: {inserted_this_batch} 条")
        logger.info(f"总计已插入: {self.progress['total_inserted']} 条")
        logger.info(f"已完成: {len(self.progress['completed_stocks'])}/{len(all_stocks)} 只")
        
        # 本批完成后休息
        logger.info(f"休息 {self.inter_batch_pause} 秒后可以重启...")
        time.sleep(self.inter_batch_pause)
        
        return False  # 返回False表示还有未完成的
 
    def close(self):
        self.save_progress()
        self.client.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='断点续传批次采集')
    parser.add_argument('--start', type=str, default='2026-02-24', help='开始日期')
    parser.add_argument('--end', type=str, default='2026-04-04', help='结束日期')
    parser.add_argument('--batch-size', type=int, default=100, help='每批股票数')
    parser.add_argument('--pause', type=int, default=60, help='批次间休息秒数')
    args = parser.parse_args()
    
    collector = BatchCollector()
    collector.batch_size = args.batch_size
    collector.inter_batch_pause = args.pause
    
    completed = collector.run_batch(args.start, args.end)
    
    collector.close()
    
    if completed:
        print("\n✅ 全部采集完成!")
    else:
        print("\n批次完成，下次运行会继续。")


if __name__ == '__main__':
    main()
