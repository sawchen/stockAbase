"""
数据获取模块 - 从各数据源获取股票信息
"""

import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import time
import json
import re

from src.models import DataSource


class DataFetcher:
    """数据获取基类"""
    
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def _fetch(self, url: str, params: Dict = None, method: str = 'GET', **kwargs) -> Dict:
        """统一请求方法"""
        start_time = time.time()
        try:
            if method == 'GET':
                response = self.session.get(url, params=params, timeout=self.timeout, **kwargs)
            else:
                response = self.session.post(url, data=params, timeout=self.timeout, **kwargs)
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            return {
                'status_code': response.status_code,
                'data': response.json(),
                'duration_ms': duration_ms,
                'url': response.url
            }
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return {
                'status_code': 0,
                'error': str(e),
                'duration_ms': duration_ms,
                'url': url
            }


class EastMoneyFetcher(DataFetcher):
    """东方财富数据获取"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://datacenter.eastmoney.com"
    
    def get_stock_info(self, symbol: str) -> Dict[str, Any]:
        """获取股票基本信息"""
        # 东方财富股票代码转换：sh.600000 -> 600000，sz.000001 -> 000001
        code = symbol.split('.')[1] if '.' in symbol else symbol
        market_map = {'sh': '1', 'sz': '0'}
        market = market_map.get(symbol.split('.')[0].lower(), '1')
        
        url = f"{self.base_url}/api/data/v1/get"
        params = {
            'reportName': 'RPT_F10_BASIC_ORGINFO',
            'columns': 'SECURITY_CODE,SECURITY_NAME_ABBR,ORG_NAME,BUSINESS_REG_ADDRESS,REG_ADDRESS',
            'filter': f'(SECURITY_CODE="{code}")'
        }
        
        result = self._fetch(url, params)
        if result['status_code'] == 200 and isinstance(result['data'], dict):
            records = result['data'].get('result', {}).get('data', [])
            return records[0] if records else {}
        return {}
    
    def get_research_reports(self, symbol: str, months: int = 24) -> List[Dict]:
        """获取研报列表 - 使用东方财富reportapi"""
        code = symbol.split('.')[1] if '.' in symbol else symbol
        
        # 正确的研报API: reportapi.eastmoney.com/report/list
        url = "https://reportapi.eastmoney.com/report/list"
        end_date = datetime.now()
        begin_date = end_date - timedelta(days=months * 30)
        params = {
            'pageSize': '100',
            'pageNo': '1',
            'qType': '0',  # 0=搜索所有研报
            'code': code,
            'beginTime': begin_date.strftime('%Y-%m-%d'),
            'endTime': end_date.strftime('%Y-%m-%d')
        }
        
        result = self._fetch(url, params)
        if result['status_code'] == 200 and isinstance(result['data'], dict):
            records = result['data'].get('data', [])
            return records
        return []
    
    def get_announcements(self, symbol: str, days: int = 30) -> List[Dict]:
        """获取近30天公告 - 使用东方财富np-anotice-stock API"""
        code = symbol.split('.')[1] if '.' in symbol else symbol
        
        # 正确的公告API: np-anotice-stock.eastmoney.com/api/security/ann
        url = "https://np-anotice-stock.eastmoney.com/api/security/ann"
        params = {
            'sr': '-1',        # 按时间倒序
            'page_size': '50',
            'page_index': '1',
            'ann_type': 'A',   # A股公告
            'stock_list': code
        }
        
        result = self._fetch(url, params)
        if result['status_code'] == 200 and isinstance(result['data'], dict):
            records = result['data'].get('data', {}).get('list', [])
            cutoff_date = datetime.now() - timedelta(days=days)
            filtered = []
            for r in records:
                notice_date_str = r.get('notice_date', '')
                if notice_date_str:
                    try:
                        notice_date = datetime.strptime(notice_date_str[:10], '%Y-%m-%d')
                        if notice_date > cutoff_date:
                            filtered.append(r)
                    except (ValueError, TypeError):
                        pass
            return filtered
        return []
    
    def get_major_shareholders(self, symbol: str) -> List[Dict]:
        """获取主要股东信息"""
        code = symbol.split('.')[1] if '.' in symbol else symbol
        
        url = f"{self.base_url}/api/data/v1/get"
        params = {
            'reportName': 'RPT_F10_SHAREHOLDERLIST',
            'columns': 'HOLDER_NAME,HOLDER_TYPE,HOLD_NUM,HOLD_RATIO,CHANGE_NUM',
            'filter': f'(SECURITY_CODE="{code}")',
            'pageNumber': '1',
            'pageSize': '10',
            'sortColumns': 'HOLD_RATIO',
            'sortTypes': '-1'
        }
        
        result = self._fetch(url, params)
        if result['status_code'] == 200 and isinstance(result['data'], dict):
            return result['data'].get('result', {}).get('data', [])
        return []
    
    def get_financial_data(self, symbol: str) -> Dict[str, Any]:
        """获取财务数据"""
        code = symbol.split('.')[1] if '.' in symbol else symbol
        
        url = f"{self.base_url}/api/data/v1/get"
        params = {
            'reportName': 'RPT_FCI_PERIOD_MAININDEX',
            'columns': 'REPORT_DATE,GROSS_MARGIN,NET_MARGIN,DEBT_ASSET_RATIO,TOTAL_REVENUE,NET_PROFIT',
            'filter': f'(SECURITY_CODE="{code}")',
            'pageNumber': '1',
            'pageSize': '4',  # 近4个季度
            'sortColumns': 'REPORT_DATE',
            'sortTypes': '-1'
        }
        
        result = self._fetch(url, params)
        if result['status_code'] == 200 and isinstance(result['data'], dict):
            return result['data'].get('result', {}).get('data', [])
        return []
    
    def get_pledge_info(self, symbol: str) -> Dict[str, Any]:
        """获取质押信息"""
        code = symbol.split('.')[1] if '.' in symbol else symbol
        
        url = f"{self.base_url}/api/data/v1/get"
        params = {
            'reportName': 'RPT_F10_MAIN_PLEDGE',
            'columns': 'PLEDGE_RATIO,PLEDGE_AMOUNT,UNFREEZE_NUM,FREEZE_NUM',
            'filter': f'(SECURITY_CODE="{code}")'
        }
        
        result = self._fetch(url, params)
        if result['status_code'] == 200 and isinstance(result['data'], dict):
            records = result['data'].get('result', {}).get('data', [])
            return records[0] if records else {}
        return {}


class TonghuashunFetcher(DataFetcher):
    """同花顺数据获取"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://d.10jqka.com.cn"
    
    def get_董秘互动(self, symbol: str, days: int = 60) -> List[Dict]:
        """
        获取董秘互动问答

        TODO: 同花顺 d.10jqka.com.cn API已废弃(v6/interactive/...返回404)，
        巨潮/东方财富董秘互动接口需要登录验证或专用API Key，
        暂无可直接调用的免费公开接口。
        替代方案：(1)使用selenium模拟登录巨潮获取数据 (2)购买数据源API
        """
        # 同花顺股票代码
        code = symbol.split('.')[1] if '.' in symbol else symbol

        # TODO: 原URL https://d.10jqka.com.cn/v6/interactive/{code}/last30.json 已废弃(404)
        # 巨潮irm.cninfo.com.cn需POST+登录；东方财富无免费董秘Q&A接口
        url = f"{self.base_url}/v6/interactive/{code}/last30.json"
        result = self._fetch(url)

        if result['status_code'] == 200:
            return result['data'] if isinstance(result['data'], list) else []
        return []
    
    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """获取公司基本信息"""
        code = symbol.split('.')[1] if '.' in symbol else symbol
        
        url = f"https://basic.10jqka.com.cn/{code}/company.html"
        # 需要解析HTML
        result = self._fetch(url)
        
        if result['status_code'] == 200:
            # 简化处理，返回原始文本供后续解析
            return {'raw_html': result['data'][:5000] if isinstance(result['data'], str) else ''}
        return {}


class JuchaoFetcher(DataFetcher):
    """juchao数据获取"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "http://www.cninfo.com.cn"
    
    def get_年报季报(self, symbol: str) -> List[Dict]:
        """获取年报季报列表"""
        # 巨潮股票代码
        code = symbol.split('.')[1] if '.' in symbol else symbol
        market_map = {'sh': 'sh', 'sz': 'sz'}
        market = market_map.get(symbol.split('.')[0].lower(), 'sh')
        
        url = f"{self.base_url}/new/disclosure/lists"
        params = {
            'stock': f'{code},{market}',
            'pageNum': '1',
            'pageSize': '20',
            'column': 'szse' if market == 'sz' else 'sse',
            'category': 'category_ndbg_szsh',
            'plate': '',
            'seDate': '',
            'searchkey': '',
            'secid': '',
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true'
        }
        
        # cninfo API 需要 POST 请求，响应结构为 {data: {...}, code: "00", ...}
        result = self._fetch(url, params, method='POST')
        if result['status_code'] == 200 and isinstance(result['data'], dict):
            # 响应格式: {"data": {"announcements": [...], "totalPages": N}, "code": "00", ...}
            data_wrapper = result['data'].get('data', {})
            return data_wrapper.get('announcements', []) if isinstance(data_wrapper, dict) else []
        return []


class QuoteFetcher(DataFetcher):
    """quote数据获取（Yahoo Finance等）"""
    
    def __init__(self):
        super().__init__()
    
    def get_price_trend(self, symbol: str, days: int = 60) -> Dict[str, Any]:
        """获取价格趋势数据"""
        # 转换为Yahoo Finance格式
        # sh.600000 -> 600000.ss, sz.000001 -> 000001.sz
        code = symbol.split('.')[1] if '.' in symbol else symbol
        suffix = 'ss' if symbol.startswith('sh') else 'sz'
        yahoo_symbol = f"{code}.{suffix}"
        
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
        params = {
            'range': f'{days}d',
            'interval': '1d'
        }
        
        result = self._fetch(url, params)
        if result['status_code'] == 200 and isinstance(result['data'], dict):
            chart = result['data'].get('chart', {}).get('result', [])
            if chart:
                return chart[0]
        return {}
    
    def get_ma_trend(self, symbol: str) -> Dict[str, Any]:
        """计算均线趋势"""
        price_data = self.get_price_trend(symbol, days=120)
        
        if not price_data:
            return {'trend': 'unknown', 'ma5': 0, 'ma10': 0, 'ma20': 0, 'ma60': 0}
        
        closes = price_data.get('indicators', {}).get('quote', [{}])[0].get('close', [])
        if not closes:
            return {'trend': 'unknown', 'ma5': 0, 'ma10': 0, 'ma20': 0, 'ma60': 0}
        
        # 计算简单移动平均
        def ma(data, period):
            if len(data) < period:
                return 0
            return sum(data[-period:]) / period
        
        ma5 = ma(closes, 5)
        ma10 = ma(closes, 10)
        ma20 = ma(closes, 20)
        ma60 = ma(closes, 60) if len(closes) >= 60 else ma(closes, len(closes))
        
        # 判断趋势
        if ma5 > ma10 > ma20:
            trend = 'bullish'
        elif ma5 < ma10 < ma20:
            trend = 'bearish'
        else:
            trend = 'mixed'
        
        return {
            'trend': trend,
            'ma5': round(ma5, 2),
            'ma10': round(ma10, 2),
            'ma20': round(ma20, 2),
            'ma60': round(ma60, 2),
            'current_price': closes[-1] if closes else 0
        }
    
    def get_macd(self, symbol: str) -> Dict[str, Any]:
        """计算MACD指标"""
        price_data = self.get_price_trend(symbol, days=120)
        
        if not price_data:
            return {'signal': 'unknown', 'macd': 0, 'signal_line': 0, 'histogram': 0}
        
        closes = price_data.get('indicators', {}).get('quote', [{}])[0].get('close', [])
        if not closes or len(closes) < 26:
            return {'signal': 'unknown', 'macd': 0, 'signal_line': 0, 'histogram': 0}
        
        # 简化MACD计算（实际应使用EMA）
        # 这里用简单移动平均近似
        ema12 = sum(closes[-12:]) / 12
        ema26 = sum(closes[-26:]) / 26
        macd = ema12 - ema26
        signal = macd * 0.9  # 简化signal线
        histogram = macd - signal
        
        if histogram > 0 and histogram > signal * 0.1:
            signal_type = 'strong_bullish'
        elif histogram > 0:
            signal_type = 'bullish'
        elif histogram < 0 and abs(histogram) > abs(signal) * 0.1:
            signal_type = 'strong_bearish'
        else:
            signal_type = 'bearish'
        
        return {
            'signal': signal_type,
            'macd': round(macd, 4),
            'signal_line': round(signal, 4),
            'histogram': round(histogram, 4)
        }


class DataFetcherFactory:
    """数据获取器工厂"""
    
    @staticmethod
    def create_fetcher(fetcher_type: str) -> DataFetcher:
        """创建数据获取器"""
        fetchers = {
            'eastmoney': EastMoneyFetcher,
            '同花顺': 同花顺Fetcher,
            'juchao': juchaoFetcher,
            'quote': quoteFetcher
        }
        
        fetcher_class = fetchers.get(fetcher_type)
        if fetcher_class:
            return fetcher_class()
        raise ValueError(f"Unknown fetcher type: {fetcher_type}")
    
    @staticmethod
    def get_all_fetcher_names() -> List[str]:
        """获取所有数据获取器名称"""
        return ['eastmoney', '同花顺', 'juchao', 'quote']
