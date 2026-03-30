"""
ClickHouse数据库操作模块
"""

import clickhouse_driver
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import uuid

from models import EvaluationResult, DimensionScore, DataSource


class StockEvalDB:
    """评估数据库操作类"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str = "stock_eval"):
        self.client = clickhouse_driver.Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        self.database = database
    
    def init_database(self):
        """初始化数据库（执行建表SQL）"""
        # 数据库已通过外部SQL初始化，这里仅验证连接
        self.client.execute("SELECT 1")
        print(f"✅ 连接数据库成功: {self.database}")
    
    def save_evaluation_result(self, result: EvaluationResult) -> str:
        """保存评估结果"""
        eval_id = result.eval_id or str(uuid.uuid4())
        
        # 插入汇总结果
        self.client.execute("""
            INSERT INTO stock_eval.evaluation_results (
                eval_id, symbol, stock_name, eval_time, eval_date,
                total_score, total满分, score_ratio, rating,
                score_信息披露, score_股权结构, score_业务清晰度,
                score_财务质量, score_股东行为, score_监管法律,
                score_行业景气, score_生产经营, score_技术面, score_外部环境
            ) VALUES
        """, (
            eval_id,
            result.symbol,
            result.stock_name,
            result.eval_time,
            datetime.date(result.eval_time),
            result.total_score,
            result.max_total_score,
            result.score_ratio,
            result.rating.value if hasattr(result.rating, 'value') else str(result.rating),
            result.score_信息披露,
            result.score_股权结构,
            result.score_业务清晰度,
            result.score_财务质量,
            result.score_股东行为,
            result.score_监管法律,
            result.score_行业景气,
            result.score_生产经营,
            result.score_技术面,
            result.score_外部环境
        ))
        
        return eval_id
    
    def save_dimension_scores(self, eval_id: str, dimensions: List[DimensionScore], symbol: str, stock_name: str):
        """保存维度评分明细"""
        for dim in dimensions:
            source_urls = "|".join([s.url for s in dim.data_sources])
            source_names = "|".join([s.name for s in dim.data_sources])
            
            self.client.execute("""
                INSERT INTO stock_eval.evaluation_details (
                    eval_id, symbol, stock_name, eval_time,
                    dim_id, dim_name, dim_category,
                    score, max_score, eval_method, raw_value, threshold_desc,
                    data_sources, source_urls, fetch_time, remark
                ) VALUES
            """, (
                eval_id,
                symbol,
                stock_name,
                datetime.now(),
                dim.dim_id,
                dim.dim_name,
                dim.dim_category,
                dim.score,
                dim.max_score,
                dim.eval_method,
                dim.raw_value,
                dim.threshold_desc,
                source_names,
                source_urls,
                datetime.now(),
                dim.remark
            ))
    
    def log_data_fetch(self, symbol: str, stock_name: str, source_name: str,
                       source_type: str, request_url: str, response_code: int,
                       data_count: int, status: str, duration_ms: int,
                       error_msg: str = None):
        """记录数据获取日志"""
        self.client.execute("""
            INSERT INTO stock_eval.data_fetch_log (
                fetch_id, fetch_time, symbol, stock_name,
                source_name, source_type, request_url,
                response_code, data_count, status, error_msg, duration_ms
            ) VALUES
        """, (
            str(uuid.uuid4()),
            datetime.now(),
            symbol,
            stock_name,
            source_name,
            source_type,
            request_url,
            response_code,
            data_count,
            status,
            error_msg,
            duration_ms
        ))
    
    def get_latest_evaluation(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新的评估结果"""
        result = self.client.execute("""
            SELECT eval_id, symbol, stock_name, eval_time, total_score, rating,
                   score_信息披露, score_股权结构, score_财务质量
            FROM stock_eval.evaluation_results
            WHERE symbol = %s
            ORDER BY eval_time DESC
            LIMIT 1
        """, (symbol,))
        
        if result:
            return {
                'eval_id': result[0][0],
                'symbol': result[0][1],
                'stock_name': result[0][2],
                'eval_time': result[0][3],
                'total_score': result[0][4],
                'rating': result[0][5],
                'score_信息披露': result[0][6],
                'score_股权结构': result[0][7],
                'score_财务质量': result[0][8]
            }
        return None
    
    def get_evaluation_history(self, symbol: str, limit: int = 10) -> List[Dict[str, Any]]:
        """获取评估历史"""
        results = self.client.execute("""
            SELECT symbol, stock_name, eval_date, total_score, rating
            FROM stock_eval.evaluation_results
            WHERE symbol = %s
            ORDER BY eval_time DESC
            LIMIT %s
        """, (symbol, limit))
        
        return [
            {
                'symbol': r[0],
                'stock_name': r[1],
                'eval_date': r[2],
                'total_score': r[3],
                'rating': r[4]
            }
            for r in results
        ]
    
    def get_dimension_details(self, eval_id: str) -> List[Dict[str, Any]]:
        """获取评估明细"""
        results = self.client.execute("""
            SELECT dim_id, dim_name, dim_category, score, max_score,
                   eval_method, raw_value, threshold_desc, source_urls, remark
            FROM stock_eval.evaluation_details
            WHERE eval_id = %s
            ORDER BY dim_id
        """, (eval_id,))
        
        return [
            {
                'dim_id': r[0],
                'dim_name': r[1],
                'dim_category': r[2],
                'score': r[3],
                'max_score': r[4],
                'eval_method': r[5],
                'raw_value': r[6],
                'threshold_desc': r[7],
                'source_urls': r[8].split('|') if r[8] else [],
                'remark': r[9]
            }
            for r in results
        ]
    
    def close(self):
        """关闭连接"""
        self.client.disconnect()
