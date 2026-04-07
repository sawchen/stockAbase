#!/usr/bin/env python3
"""
专项补数脚本：处理首批失败的37只股票
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import clickhouse_connect
import baostock as bs
import pandas as pd
import yaml
import logging
from datetime import datetime
import time
import json

WORKDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(WORKDIR, 'backfill_37.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

FAILED_STOCKS = [
    '600290.SH','000005.SZ','603555.SH','000961.SZ','300495.SZ','300116.SZ',
    '000971.SZ','300262.SZ','000982.SZ','600321.SH','600766.SH','300282.SZ',
    '600277.SH','300742.SZ','300799.SZ','600565.SH','600112.SH','600297.SH',
    '123085.SZ','123119.SZ','123124.SZ','123146.SZ','123223.SZ',
    '127033.SZ','127049.SZ','127059.SZ','127067.SZ','127075.SZ',
    '127076.SZ','127084.SZ','128128.SZ','399007.SZ','399012.SZ',
    '603056.SH','002231.SZ','123176.SH','123195.SH'
]

START_DATE = '2026-02-14'
END_DATE = '2026-04-04'

def symbol_to_baostock(symbol):
    if symbol.endswith('.SH'): return 'sh.' + symbol.split('.')[0]
    if symbol.endswith('.SZ'): return 'sz.' + symbol.split('.')[0]
    return None

def get_last_date(client, symbol):
    try:
        result = client.query(
            'SELECT max(trade_date) FROM stock_data.stock_minute WHERE symbol = %s',
            parameters=(symbol,)
        )
        rows = result.result_rows
        if rows and rows[0][0]:
            return str(rows[0][0])[:10]
    except: pass
    return None

def parse_time(time_str):
    return datetime(
        int(time_str[0:4]), int(time_str[4:6]), int(time_str[6:8]),
        int(time_str[8:10]), int(time_str[10:12]), int(time_str[12:14])
    )

def process_stock(client, bs, symbol):
    bs_symbol = symbol_to_baostock(symbol)
    if not bs_symbol:
        logger.warning(f"{symbol}: 无效格式")
        return 0

    last_date = get_last_date(client, symbol)
    if last_date and last_date >= END_DATE:
        logger.info(f"{symbol}: DB最新{last_date} >= {END_DATE}，已完整")
        return 0

    start = START_DATE if not last_date else last_date
    logger.info(f"{symbol}: 从DB最新{last_date}开始，获取{start}~{END_DATE}")

    for retry in range(3):
        try:
            rs = bs.query_history_k_data_plus(
                bs_symbol,
                'date,time,open,high,low,close,volume',
                start_date=start, end_date=END_DATE,
                frequency='5', adjustflag='2'
            )
            if rs.error_code == '0': break
            logger.warning(f"{symbol} 查询失败 ({retry+1}/3): {rs.error_msg}")
            time.sleep(60)
        except Exception as e:
            logger.warning(f"{symbol} 异常 ({retry+1}/3): {e}")
            time.sleep(60)
    else:
        logger.error(f"{symbol}: 3次重试均失败")
        return 0

    data_list = []
    try:
        while rs.next():
            data_list.append(rs.get_row_data())
    except Exception as e:
        logger.warning(f"{symbol} 读取数据异常: {e}")

    if not data_list:
        logger.warning(f"{symbol}: 无数据")
        return 0

    df = pd.DataFrame(data_list, columns=rs.fields)
    rows = []
    trade_date = None
    for _, row in df.iterrows():
        dt = parse_time(row['time'])
        if trade_date is None:
            trade_date = dt.date()
        rows.append([
            symbol,
            trade_date,
            dt,
            dt,  # trade_time_cst
            float(row['open']),
            float(row['high']),
            float(row['low']),
            float(row['close']),
            int(float(row['volume'])),
            0.0,  # amount
            datetime.now(),
        ])

    column_names = ['symbol','trade_date','trade_time','trade_time_cst','open','high','low','close','volume','amount','created_at']
    try:
        client.insert('stock_data.stock_minute', rows, column_names=column_names)
        logger.info(f"{symbol}: 插入 {len(rows)} 条")
        return len(rows)
    except Exception as e:
        logger.error(f"{symbol} 插入失败: {e}")
        return 0

def main():
    with open(os.path.join(WORKDIR, 'config.yaml')) as f:
        db_conf = yaml.safe_load(f)['clickhouse']
    client = clickhouse_connect.get_client(
        host=db_conf['host'], port=db_conf['port'],
        user=db_conf['user'], password=db_conf['password'],
        database='stock_data'
    )

    bs.login()
    logger.info(f"开始补数 {len(FAILED_STOCKS)} 只股票，范围 {START_DATE}~{END_DATE}")

    total = 0
    for i, symbol in enumerate(FAILED_STOCKS):
        logger.info(f"[{i+1}/{len(FAILED_STOCKS)}] 处理 {symbol}")
        n = process_stock(client, bs, symbol)
        total += n
        time.sleep(0.1)

    logger.info(f"补数完成，共插入 {total} 条")
    bs.logout()
    client.close()

if __name__ == '__main__':
    main()
