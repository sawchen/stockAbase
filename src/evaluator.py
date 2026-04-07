"""
评估器模块 - 实现24维度量化评估逻辑
"""

import uuid
import logging
from datetime import datetime
from typing import Dict, List, Any

from src.models import (
    EvaluationResult, DimensionScore, DataSource, Rating,
    get_rating, DIMENSION_CATEGORIES
)
from src.data_fetcher import EastMoneyFetcher, TonghuashunFetcher, JuchaoFetcher, QuoteFetcher


class StockEvaluator:
    """股票评估器"""
    
    def __init__(self):
        self.eastmoney = EastMoneyFetcher()
        self.tonghuashun = TonghuashunFetcher()
        self.juchao = JuchaoFetcher()
        self.quote = QuoteFetcher()
        
        self.知名机构名单 = [
            '证金', '汇金', '社保', '公募', '私募', '保险', '外资',
            '高盛', '摩根', '瑞银', '野村', '摩根士丹利', '花旗'
        ]
        
        # 配置日志
        self.logger = logging.getLogger(__name__)
    
    def evaluate(self, symbol: str, stock_name: str = None) -> EvaluationResult:
        """执行全面评估"""
        eval_id = str(uuid.uuid4())
        
        if not stock_name:
            stock_info = self.eastmoney.get_stock_info(symbol)
            stock_name = stock_info.get('SECURITY_NAME_ABBR', symbol)
        
        raw_data = self._collect_raw_data(symbol)
        dimensions = []
        
        for dim_id in range(1, 25):
            dim_score = self._evaluate_dimension(dim_id, symbol, raw_data)
            dimensions.append(dim_score)
        
        category_scores = self._calculate_category_scores(dimensions)
        total_score = sum(d.score for d in dimensions)
        score_ratio = total_score / 120.0
        
        result = EvaluationResult(
            eval_id=eval_id,
            symbol=symbol,
            stock_name=stock_name,
            eval_time=datetime.now(),
            total_score=total_score,
            rating=get_rating(score_ratio),
            dimensions=dimensions,
            **category_scores
        )
        
        return result
    
    def _collect_raw_data(self, symbol: str) -> Dict[str, Any]:
        """收集所有原始数据，带异常保护"""
        data = {}
        source_status = {}  # 跟踪每个数据源的状态
        fetchers = [
            ('stock_info',         lambda: self.eastmoney.get_stock_info(symbol),          '东方财富-股票信息'),
            ('research_reports',   lambda: self.eastmoney.get_research_reports(symbol, months=24), '东方财富-研报'),
            ('announcements',      lambda: self.eastmoney.get_announcements(symbol, days=30),   '东方财富-公告'),
            ('major_shareholders', lambda: self.eastmoney.get_major_shareholders(symbol),     '东方财富-主要股东'),
            ('financial_data',     lambda: self.eastmoney.get_financial_data(symbol),        '东方财富-财务数据'),
            ('pledge_info',        lambda: self.eastmoney.get_pledge_info(symbol),          '东方财富-质押信息'),
            ('董秘互动',            lambda: self.tonghuashun.get_董秘互动(symbol, days=60),       '同花顺-董秘互动'),
            ('年报季报',            lambda: self.juchao.get_年报季报(symbol),                    '巨潮资讯-年报季报'),
            ('ma_trend',           lambda: self.quote.get_ma_trend(symbol),                  'Yahoo-均线趋势'),
            ('macd',               lambda: self.quote.get_macd(symbol),                     'Yahoo-MACD'),
        ]
        
        for key, fetcher_fn, source_name in fetchers:
            try:
                result = fetcher_fn()
                data[key] = result
                source_status[key] = {'status': 'success', 'source': source_name}
                self.logger.info(f"[数据获取成功] {source_name} ({key}), 结果数: {len(result) if hasattr(result, '__len__') else 'N/A'}")
            except Exception as e:
                data[key] = None
                source_status[key] = {'status': 'failed', 'source': source_name, 'error': str(e)}
                self.logger.warning(f"[数据获取失败] {source_name} ({key}): {e}")
        
        # 将数据源状态也放入data中，供维度评估器判断"有数据"vs"无数据"
        data['__data_source_status__'] = source_status
        self.logger.info(f"[数据采集完成] 成功: {sum(1 for s in source_status.values() if s['status']=='success')}/{len(source_status)}, 股票: {symbol}")
        return data
    
    def _evaluate_dimension(self, dim_id: int, symbol: str, raw_data: Dict) -> DimensionScore:
        category, dim_name = DIMENSION_CATEGORIES.get(dim_id, ("未知", "未知"))
        eval_method = getattr(self, f'_evaluate_dim_{dim_id}', self._evaluate_default)
        return eval_method(dim_id, category, dim_name, symbol, raw_data)
    
    def _evaluate_default(self, dim_id: int, category: str, dim_name: str, 
                         symbol: str, raw_data: Dict) -> DimensionScore:
        return DimensionScore(
            dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=3.0, eval_method="默认评估", raw_value="无数据",
            threshold_desc="无法获取数据，默认3分"
        )
    
    def _calculate_category_scores(self, dimensions: List[DimensionScore]) -> Dict[str, float]:
        scores = {
            'score_信息披露': 0.0, 'score_股权结构': 0.0, 'score_业务清晰度': 0.0,
            'score_财务质量': 0.0, 'score_股东行为': 0.0, 'score_监管法律': 0.0,
            'score_行业景气': 0.0, 'score_生产经营': 0.0, 'score_技术面': 0.0,
            'score_外部环境': 0.0
        }
        category_map = {
            '信息披露': 'score_信息披露', '股权结构': 'score_股权结构',
            '业务清晰度': 'score_业务清晰度', '财务质量': 'score_财务质量',
            '股东行为': 'score_股东行为', '监管法律': 'score_监管法律',
            '行业景气': 'score_行业景气', '生产经营': 'score_生产经营',
            '技术面': 'score_技术面', '外部环境': 'score_外部环境'
        }
        for dim in dimensions:
            key = category_map.get(dim.dim_category)
            if key:
                scores[key] += dim.score
        return scores
    
    def _evaluate_dim_1(self, dim_id, category, dim_name, symbol, raw_data):
        announcements = raw_data.get('announcements', [])
        recent_count = len(announcements)
        if recent_count >= 10: score, desc = 5.0, f"近30天有{recent_count}条公告"
        elif recent_count >= 5: score, desc = 4.0, f"近30天有{recent_count}条公告"
        elif recent_count >= 3: score, desc = 3.0, f"近30天有{recent_count}条公告"
        elif recent_count >= 1: score, desc = 2.0, f"近30天有{recent_count}条公告"
        else: score, desc = 1.0, "近30天无公告"
        url = f"https://np-anotice-stock.eastmoney.com/api/security/ann"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="统计近30天公告数量", raw_value=f"数量: {recent_count}",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url=url, fetch_time=datetime.now())])
    
    def _evaluate_dim_2(self, dim_id, category, dim_name, symbol, raw_data):
        qas = raw_data.get('董秘互动', [])
        if not qas: return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=0.0, eval_method="统计董秘互动", raw_value="0次", threshold_desc="近60天无董秘互动")
        avg_len = sum(len(q.get('content', '')) for q in qas) / len(qas) if qas else 0
        if len(qas) >= 10 and avg_len > 50: score, desc = 5.0, f"回复{len(qas)}次，质量高"
        elif len(qas) >= 5: score, desc = 4.0, f"回复{len(qas)}次，质量较好"
        elif len(qas) >= 1: score, desc = 3.0, f"回复{len(qas)}次"
        else: score, desc = 1.0, "回复稀少"
        url = f"https://d.10jqka.com.cn/v6/interactive/{symbol.split('.')[1]}/last30.json"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="统计董秘互动", raw_value=f"次数: {len(qas)}",
            threshold_desc=desc, data_sources=[DataSource(name="同花顺", url=url, fetch_time=datetime.now())])
    
    def _evaluate_dim_3(self, dim_id, category, dim_name, symbol, raw_data):
        reports = raw_data.get('research_reports', [])
        cnt = len(reports)
        if cnt >= 20: score, desc = 5.0, f"近24月{cnt}篇，覆盖充分"
        elif cnt >= 10: score, desc = 4.0, f"近24月{cnt}篇，覆盖较好"
        elif cnt >= 5: score, desc = 3.0, f"近24月{cnt}篇"
        elif cnt >= 1: score, desc = 2.0, f"近24月{cnt}篇"
        else: score, desc = 0.0, "24月无研报"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="统计研报数量", raw_value=f"数量: {cnt}",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url="https://datacenter.eastmoney.com", fetch_time=datetime.now())])
    
    def _evaluate_dim_4(self, dim_id, category, dim_name, symbol, raw_data):
        shareholders = raw_data.get('major_shareholders', [])
        if not shareholders: return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=2.0, eval_method="分析股东比例", raw_value="无数据", threshold_desc="无法获取股东信息")
        ratio = shareholders[0].get('HOLD_RATIO', 0)
        if ratio >= 30: score, desc = 5.0, f"实控人持股{ratio}%，控制力强"
        elif ratio >= 15: score, desc = 4.0, f"实控人持股{ratio}%"
        elif ratio >= 5: score, desc = 3.0, f"实控人持股{ratio}%"
        elif ratio >= 1: score, desc = 2.0, f"实控人持股{ratio}%"
        else: score, desc = 0.0, f"实控人持股极少"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="分析股东比例", raw_value=f"第一大股东: {ratio}%",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url="https://datacenter.eastmoney.com", fetch_time=datetime.now())])
    
    def _evaluate_dim_5(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=3.0, eval_method="需年报数据", raw_value="不可获取",
            threshold_desc="暂无可获取的董事长年龄和持股数据", remark="建议手动查阅年报")
    
    def _evaluate_dim_6(self, dim_id, category, dim_name, symbol, raw_data):
        shareholders = raw_data.get('major_shareholders', [])
        known = sum(1 for sh in shareholders for k in self.知名机构名单 if k in sh.get('HOLDER_NAME', ''))
        if known >= 5: score, desc = 5.0, f"知名机构{known}家"
        elif known >= 3: score, desc = 4.0, f"知名机构{known}家"
        elif known >= 1: score, desc = 3.0, f"知名机构{known}家"
        else: score, desc = 1.0, "无知名机构"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="统计知名机构", raw_value=f"数量: {known}",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url="https://datacenter.eastmoney.com", fetch_time=datetime.now())])
    
    def _evaluate_dim_7(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=3.0, eval_method="需年报数据", raw_value="不可获取",
            threshold_desc="暂无可获取的主营业务数据", remark="建议手动查阅年报")
    
    def _evaluate_dim_8(self, dim_id, category, dim_name, symbol, raw_data):
        fd = raw_data.get('financial_data', [])
        if not fd: return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=2.0, eval_method="分析净利润", raw_value="无数据", threshold_desc="无法获取财务数据")
        profit = float(fd[0].get('NET_PROFIT', 0)) if fd else 0
        score, desc = (4.0, f"净利润正: {profit}") if profit > 0 else (1.0, "净利润亏损")
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="分析净利润", raw_value=f"净利润: {profit}",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url="https://datacenter.eastmoney.com", fetch_time=datetime.now())])
    
    def _evaluate_dim_9(self, dim_id, category, dim_name, symbol, raw_data):
        fd = raw_data.get('financial_data', [])
        pledge = raw_data.get('pledge_info', {})
        debt = float(fd[0].get('DEBT_ASSET_RATIO', 0)) * 100 if fd and fd[0].get('DEBT_ASSET_RATIO') else None
        pledge_ratio = pledge.get('PLEDGE_RATIO', 0)
        if debt is None: score, desc = 2.5, "无法获取数据"
        elif debt < 40: score, desc = 5.0, f"资产负债率{debt:.1f}%"
        elif debt < 50: score, desc = 4.0, f"资产负债率{debt:.1f}%"
        elif debt < 60: score, desc = 3.0, f"资产负债率{debt:.1f}%"
        elif debt < 70: score, desc = 2.0, f"资产负债率{debt:.1f}%"
        else: score, desc = 1.0, f"资产负债率{debt:.1f}%"
        if pledge_ratio and float(pledge_ratio) > 50: score = min(score, 2.0)
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="分析资产负债率", raw_value=f"负债率: {debt}%, 质押: {pledge_ratio}%",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url="https://datacenter.eastmoney.com", fetch_time=datetime.now())])
    
    def _evaluate_dim_10(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=3.0, eval_method="需融资融券数据", raw_value="不可获取",
            threshold_desc="暂无可获取的融资比例数据", remark="建议手动查看")
    
    def _evaluate_dim_11(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=3.0, eval_method="需公告数据", raw_value="不可获取",
            threshold_desc="暂无可获取的减持数据", remark="建议手动查看公告")
    
    def _evaluate_dim_12(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=3.0, eval_method="需季报数据", raw_value="不可获取",
            threshold_desc="暂无可获取的股东人数数据", remark="建议手动查看")
    
    def _evaluate_dim_13(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=4.0, eval_method="需证监会数据", raw_value="默认无立案",
            threshold_desc="暂无可获取的立案信息，默认无立案", remark="建议手动查询")
    
    def _evaluate_dim_14(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=4.0, eval_method="需天眼查数据", raw_value="默认无诉讼",
            threshold_desc="暂无可获取的诉讼信息，默认无重大诉讼", remark="建议手动查询")
    
    def _evaluate_dim_15(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=3.0, eval_method="需行业指数数据", raw_value="不可获取",
            threshold_desc="暂无可获取的行业趋势数据", remark="建议手动查看")
    
    def _evaluate_dim_16(self, dim_id, category, dim_name, symbol, raw_data):
        reports = raw_data.get('research_reports', [])
        cnt = len(reports)
        if cnt >= 5: score, desc = 4.0, f"研报{cnt}篇，逻辑清晰"
        elif cnt >= 1: score, desc = 3.0, f"研报{cnt}篇"
        else: score, desc = 2.0, "无研报"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="分析研报覆盖", raw_value=f"数量: {cnt}",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url="https://datacenter.eastmoney.com", fetch_time=datetime.now())])
    
    def _evaluate_dim_17(self, dim_id, category, dim_name, symbol, raw_data):
        anns = raw_data.get('announcements', [])
        keywords = ['原料', '供应', '停产', '断供', '不合格']
        neg = sum(1 for a in anns if any(k in a.get('TITLE', '') for k in keywords))
        if neg == 0: score, desc = 5.0, "无供应链负面"
        elif neg <= 2: score, desc = 3.0, f"有{neg}条相关公告"
        else: score, desc = 1.0, f"有{neg}条负面公告"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="分析公告关键词", raw_value=f"负面: {neg}条",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url="https://datacenter.eastmoney.com", fetch_time=datetime.now())])
    
    def _evaluate_dim_18(self, dim_id, category, dim_name, symbol, raw_data):
        fd = raw_data.get('financial_data', [])
        gm = float(fd[0].get('GROSS_MARGIN', 0)) * 100 if fd and fd[0].get('GROSS_MARGIN') else None
        if gm is None: score, desc = 3.0, "无法获取"
        elif gm > 40: score, desc = 5.0, f"毛利率{gm:.1f}%"
        elif gm > 30: score, desc = 4.0, f"毛利率{gm:.1f}%"
        elif gm > 15: score, desc = 3.0, f"毛利率{gm:.1f}%"
        elif gm > 5: score, desc = 2.0, f"毛利率{gm:.1f}%"
        else: score, desc = 1.0, f"毛利率{gm:.1f}%"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="分析毛利率", raw_value=f"毛利率: {gm}%",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url="https://datacenter.eastmoney.com", fetch_time=datetime.now())])
    
    def _evaluate_dim_19(self, dim_id, category, dim_name, symbol, raw_data):
        ma = raw_data.get('ma_trend', {})
        trend = ma.get('trend', 'unknown')
        if trend == 'bullish': score, desc = 5.0, "多头排列"
        elif trend == 'mixed': score, desc = 3.0, "均线纠缠"
        elif trend == 'bearish': score, desc = 1.0, "空头排列"
        else: score, desc = 2.5, "无法判断"
        suffix = 'ss' if symbol.startswith('sh') else 'sz'
        yahoo_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.split('.')[1]}.{suffix}"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="分析均线", raw_value=f"MA5={ma.get('ma5',0)}, MA10={ma.get('ma10',0)}",
            threshold_desc=desc, data_sources=[DataSource(name="Yahoo Finance", url=yahoo_url, fetch_time=datetime.now())])
    
    def _evaluate_dim_20(self, dim_id, category, dim_name, symbol, raw_data):
        macd = raw_data.get('macd', {})
        sig = macd.get('signal', 'unknown')
        if sig == 'strong_bullish': score, desc = 5.0, "MACD强势"
        elif sig == 'bullish': score, desc = 4.0, "MACD向好"
        elif sig == 'bearish': score, desc = 2.0, "MACD弱势"
        elif sig == 'strong_bearish': score, desc = 1.0, "MACD大幅走弱"
        else: score, desc = 2.5, "无法判断"
        suffix = 'ss' if symbol.startswith('sh') else 'sz'
        yahoo_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.split('.')[1]}.{suffix}"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="分析MACD", raw_value=f"MACD={macd.get('macd',0)}",
            threshold_desc=desc, data_sources=[DataSource(name="Yahoo Finance", url=yahoo_url, fetch_time=datetime.now())])
    
    def _evaluate_dim_21(self, dim_id, category, dim_name, symbol, raw_data):
        reports = raw_data.get('年报季报', [])
        cnt = len(reports)
        if cnt >= 4: score, desc = 5.0, f"披露正常({cnt}份)"
        elif cnt >= 1: score, desc = 3.0, f"有{cnt}份"
        else: score, desc = 3.0, "无法获取"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="检查年报季报", raw_value=f"数量: {cnt}",
            threshold_desc=desc, data_sources=[DataSource(name="巨潮资讯", url="http://www.cninfo.com.cn", fetch_time=datetime.now())])
    
    def _evaluate_dim_22(self, dim_id, category, dim_name, symbol, raw_data):
        reports = raw_data.get('research_reports', [])
        cnt = len(reports)
        if cnt >= 5: score, desc = 5.0, f"研报{cnt}篇，偏正面"
        elif cnt >= 2: score, desc = 4.0, f"研报{cnt}篇"
        elif cnt >= 1: score, desc = 3.0, f"仅{cnt}篇"
        else: score, desc = 2.0, "无评级"
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=score, eval_method="统计研报", raw_value=f"数量: {cnt}",
            threshold_desc=desc, data_sources=[DataSource(name="东方财富", url="https://datacenter.eastmoney.com", fetch_time=datetime.now())])
    
    def _evaluate_dim_23(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=3.0, eval_method="需人工判断", raw_value="无法获取",
            threshold_desc="暂无可获取的政策舆论数据", remark="建议关注官方表态")
    
    def _evaluate_dim_24(self, dim_id, category, dim_name, symbol, raw_data):
        return DimensionScore(dim_id=dim_id, dim_name=dim_name, dim_category=category,
            score=3.0, eval_method="需人工判断", raw_value="无法获取",
            threshold_desc="暂无可获取的对美业务数据", remark="建议查看年报地区分布")
