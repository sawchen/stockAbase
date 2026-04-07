#!/usr/bin/env python3
"""
A股分钟K线断点续传批处理补数脚本
- 读取 stocks_to_backfill.txt（5228只股票）
- 每批100只，串行执行，共53批
- 检查DB最新日期，增量获取baostock数据（2026-02-14~2026-04-04）
- 插入ClickHouse，每批休息60秒
- 进度写入 backfill_progress.txt
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

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
import signal

WORKDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(WORKDIR, 'backfill_batch.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BackfillRunner:
    def __init__(self):
        self.workdir = WORKDIR
        self.config_path = os.path.join(self.workdir, 'config.yaml')
        self.stocks_file = os.path.join(self.workdir, 'stocks_to_backfill.txt')
        self.progress_file = os.path.join(self.workdir, 'backfill_progress.txt')

        self.batch_size = 100
        self.inter_batch_pause = 60   # 每批次完成后休息60秒
        self.stock_rate_pause = 0.05  # 每只股票间休息50ms（避免限流）
        self.retry_limit = 3
        self.retry_pause = 60         # 重试前休息60秒

        # 补数日期范围
        self.start_date = '2026-02-14'
        self.end_date = '2026-04-04'

        # 加载配置
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        db_conf = config['clickhouse']

        self.client = clickhouse_connect.get_client(
            host=db_conf['host'],
            port=db_conf['port'],
            user=db_conf['user'],
            password=db_conf['password'],
            database='stock_data'
        )

        # 初始化baostock
        self.bs = bs
        self.bs.login()
        logger.info("baostock 登录成功")

        # 加载进度
        self.progress = self._load_progress()

        # 注册退出保存
        atexit.register(self._save_progress)
        signal.signal(signal.SIGINT, self._sigint_handler)
        signal.signal(signal.SIGTERM, self._sigint_handler)

    def _sigint_handler(self, signum, frame):
        logger.info("收到中断信号，正在保存进度...")
        self._save_progress()
        sys.exit(0)

    def _load_progress(self):
        """加载断点进度"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                logger.info(f"从断点恢复: {data}")
                return data
            except Exception as e:
                logger.warning(f"进度文件读取失败({e})，从头开始")
        return {
            'next_index': 0,
            'done_stocks': [],       # 已完成的股票symbol列表
            'total_inserted': 0,
            'batch_results': [],     # 每批结果 [{batch, done, inserted}]
        }

    def _save_progress(self):
        """保存断点进度"""
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2, ensure_ascii=False)

    def _read_all_stocks(self):
        """读取股票列表"""
        stocks = []
        with open(self.stocks_file, 'r', encoding='utf-8') as f:
            for line in f:
                s = line.strip()
                if s:
                    stocks.append(s)
        logger.info(f"从 {self.stocks_file} 读取了 {len(stocks)} 只股票")
        return stocks

    def _get_last_date_in_db(self, symbol_db):
        """获取某只股票在DB中的最新日期"""
        try:
            result = self.client.query(
                'SELECT max(trade_date) FROM stock_data.stock_minute WHERE symbol = %s',
                parameters=(symbol_db,)
            )
            rows = result.result_rows
            if rows and rows[0][0]:
                return str(rows[0][0])[:10]  # e.g. "2026-02-13"
        except Exception as e:
            logger.debug(f"{symbol_db} 查询最后日期失败: {e}")
        return None

    def _symbol_to_baostock(self, symbol):
        """600290.SH -> sh.600290, 000005.SZ -> sz.000005"""
        if symbol.endswith('.SH'):
            return 'sh.' + symbol.split('.')[0]
        elif symbol.endswith('.SZ'):
            return 'sz.' + symbol.split('.')[0]
        return None

    def _fetch_5min_data(self, symbol_baostock, start_date, end_date):
        """获取单只股票5分钟K线数据（前复权）"""
        for retry in range(self.retry_limit):
            try:
                rs = self.bs.query_history_k_data_plus(
                    symbol_baostock,
                    'date,time,open,high,low,close,volume',
                    start_date=start_date,
                    end_date=end_date,
                    frequency='5',
                    adjustflag='2'   # 前复权
                )
                if rs.error_code == '0':
                    break
                logger.warning(f"{symbol_baostock} 查询失败 [{retry+1}/{self.retry_limit}]: {rs.error_msg}")
                if retry < self.retry_limit - 1:
                    time.sleep(self.retry_pause)
            except Exception as e:
                logger.warning(f"{symbol_baostock} 异常 [{retry+1}/{self.retry_limit}]: {e}")
                if retry < self.retry_limit - 1:
                    time.sleep(self.retry_pause)

        data_list = []
        try:
            while rs.next():
                data_list.append(rs.get_row_data())
        except Exception as e:
            logger.debug(f"{symbol_baostock} 读取数据异常: {e}")

        if not data_list:
            return None

        df = pd.DataFrame(data_list, columns=rs.fields)
        return df

    def _parse_time(self, row):
        """解析 baostock time 字段: 20260224093500 -> datetime"""
        time_str = row['time']  # format: YYYYMMDDHHMMSS
        return datetime(
            int(time_str[0:4]),
            int(time_str[4:6]),
            int(time_str[6:8]),
            int(time_str[8:10]),
            int(time_str[10:12])
        )

    def _insert_data(self, df, symbol_db):
        """批量插入数据到ClickHouse"""
        if df is None or df.empty:
            return 0

        rows = []
        trade_date = None
        for _, row in df.iterrows():
            try:
                trade_time = self._parse_time(row)
                if trade_date is None:
                    trade_date = trade_time.date()
                rows.append([
                    symbol_db,
                    trade_date,
                    trade_time,
                    trade_time,         # trade_time_cst
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(float(row['volume'])),
                    0.0,               # amount
                    datetime.now(),
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
            logger.warning(f"{symbol_db} 插入失败: {e}")
            return 0

    def run(self):
        """执行补数"""
        all_stocks = self._read_all_stocks()
        total_stocks = len(all_stocks)
        total_batches = (total_stocks + self.batch_size - 1) // self.batch_size
        next_index = self.progress['next_index']

        logger.info("=" * 60)
        logger.info(f"A股分钟K线补数任务启动")
        logger.info(f"总股票数: {total_stocks}, 每批 {self.batch_size}, 共 {total_batches} 批")
        logger.info(f"补数范围: {self.start_date} ~ {self.end_date}")
        logger.info(f"从断点 index={next_index} 继续" if next_index > 0 else "从头开始")
        logger.info("=" * 60)

        for batch_num in range(next_index, total_batches):
            start_idx = batch_num * self.batch_size
            end_idx = min(start_idx + self.batch_size, total_stocks)
            batch = all_stocks[start_idx:end_idx]

            logger.info(f"\n{'='*60}")
            logger.info(f"批次 {batch_num+1}/{total_batches} | 股票 {start_idx+1}~{end_idx}/{total_stocks} | {len(batch)} 只")
            logger.info(f"{'='*60}")

            batch_success = 0
            batch_fail = 0
            batch_inserted = 0

            for i, symbol in enumerate(batch):
                symbol_baostock = self._symbol_to_baostock(symbol)
                if not symbol_baostock:
                    logger.warning(f"跳过无效股票格式: {symbol}")
                    batch_fail += 1
                    continue

                # 检查DB最新日期
                last_date = self._get_last_date_in_db(symbol)
                if last_date and last_date >= self.end_date:
                    logger.debug(f"{symbol} DB最新({last_date}) >= 目标({self.end_date})，跳过")
                    # 如果DB已有目标范围最后日期数据，视为已完成
                    if symbol not in self.progress['done_stocks']:
                        self.progress['done_stocks'].append(symbol)
                    batch_success += 1
                    time.sleep(self.stock_rate_pause)
                    continue

                # 确定查询起始日期（DB最后日期的次日，或配置的起始日期）
                if last_date:
                    fetch_start = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    fetch_start = self.start_date

                # 如果计算出的起始日期已超过目标结束日期，跳过
                if fetch_start > self.end_date:
                    logger.debug(f"{symbol} 起始日期({fetch_start}) > 目标({self.end_date})，跳过")
                    if symbol not in self.progress['done_stocks']:
                        self.progress['done_stocks'].append(symbol)
                    batch_success += 1
                    time.sleep(self.stock_rate_pause)
                    continue

                # 获取增量数据
                df = self._fetch_5min_data(symbol_baostock, fetch_start, self.end_date)

                if df is not None and not df.empty:
                    inserted = self._insert_data(df, symbol)
                    batch_inserted += inserted
                    if inserted > 0:
                        batch_success += 1
                        if symbol not in self.progress['done_stocks']:
                            self.progress['done_stocks'].append(symbol)
                    else:
                        batch_fail += 1
                else:
                    batch_fail += 1

                self.progress['total_inserted'] += batch_inserted

                # 每只股票间微暂停
                time.sleep(self.stock_rate_pause)

                # 实时进度
                if (i + 1) % 20 == 0 or (i + 1) == len(batch):
                    done_count = len(self.progress['done_stocks'])
                    logger.info(f"  批次进度: {i+1}/{len(batch)} ({100*(i+1)//len(batch)}%) | "
                                f"本批成功:{batch_success} 失败:{batch_fail} 插入:{batch_inserted}条 | "
                                f"累计完成:{done_count}/{total_stocks} 累计插入:{self.progress['total_inserted']}条")

            # 更新进度
            self.progress['next_index'] = batch_num + 1
            self.progress['batch_results'].append({
                'batch': batch_num + 1,
                'start_idx': start_idx,
                'done': batch_success,
                'fail': batch_fail,
                'inserted': batch_inserted,
            })
            self._save_progress()

            done_count = len(self.progress['done_stocks'])
            logger.info(f"\n批次 {batch_num+1}/{total_batches} 完成")
            logger.info(f"  本批: 成功 {batch_success} 只, 失败 {batch_fail} 只, 插入 {batch_inserted} 条")
            logger.info(f"  累计: 完成 {done_count}/{total_stocks} 只, 插入 {self.progress['total_inserted']} 条")
            logger.info(f"  休息 {self.inter_batch_pause} 秒...")

            # 批次间休息
            if batch_num + 1 < total_batches:
                time.sleep(self.inter_batch_pause)

        # 全部完成
        logger.info("\n" + "=" * 60)
        logger.info("补数任务全部完成！")
        logger.info(f"总完成: {len(self.progress['done_stocks'])}/{total_stocks} 只")
        logger.info(f"总插入: {self.progress['total_inserted']} 条")
        logger.info("=" * 60)

        self._save_progress()

    def close(self):
        self.bs.logout()
        self.client.close()


if __name__ == '__main__':
    runner = BackfillRunner()
    try:
        runner.run()
    finally:
        runner.close()
