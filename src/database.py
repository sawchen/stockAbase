"""
ClickHouse数据库操作模块 - 使用clickhouse_connect库
"""

import clickhouse_connect
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import uuid

from src.models import EvaluationResult, DimensionScore, DataSource


class StockEvalDB:
    """评估数据库操作类"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str = "stock_eval"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
    
    def init_database(self):
        """初始化数据库（验证连接）"""
        result = self.client.query("SELECT 1")
        print(f"✅ 连接数据库成功: {self.database}")
    
    def save_evaluation_result(self, result: EvaluationResult) -> str:
        """保存评估结果"""
        eval_id = result.eval_id or str(uuid.uuid4())
        
        # Rating value
        rating_val = result.rating.value if hasattr(result.rating, 'value') else str(result.rating)
        
        # Build the row data as a list (matching column order exactly)
        row_data = [
            eval_id,
            result.symbol,
            result.stock_name,
            result.eval_time,                           # datetime object
            datetime.date(result.eval_time),            # date object
            float(result.total_score),
            float(result.max_total_score),
            float(result.score_ratio),
            rating_val,
            float(result.score_信息披露),
            float(result.score_股权结构),
            float(result.score_业务清晰度),
            float(result.score_财务质量),
            float(result.score_股东行为),
            float(result.score_监管法律),
            float(result.score_行业景气),
            float(result.score_生产经营),
            float(result.score_技术面),
            float(result.score_外部环境),
        ]
        
        column_names = [
            'eval_id', 'symbol', 'stock_name', 'eval_time', 'eval_date',
            'total_score', 'max_score', 'score_ratio', 'rating',
            'score_info_disclosure', 'score_ownership', 'score_business_clarity',
            'score_financial', 'score_shareholder', 'score_regulation',
            'score_industry', 'score_production', 'score_technical', 'score_external'
        ]
        
        self.client.insert(
            'stock_eval.evaluation_results',
            [row_data],
            column_names=column_names
        )
        return eval_id
    
    def save_dimension_scores(self, eval_id: str, dimensions: List[DimensionScore], symbol: str, stock_name: str):
        """保存维度评分明细"""
        column_names = [
            'eval_id', 'symbol', 'stock_name', 'eval_time',
            'dim_id', 'dim_name', 'dim_category',
            'score', 'max_score',
            'eval_method', 'raw_value', 'threshold_desc',
            'data_sources', 'source_urls', 'fetch_time', 'remark'
        ]
        
        rows = []
        for dim in dimensions:
            source_urls = "|".join([s.url.replace("'", "\\'") for s in dim.data_sources]) if dim.data_sources else ""
            source_names = "|".join([s.name for s in dim.data_sources]) if dim.data_sources else ""
            remark = dim.remark.replace("'", "\\'") if dim.remark else ""
            raw_value = dim.raw_value.replace("'", "\\'") if dim.raw_value else ""
            threshold_desc = dim.threshold_desc.replace("'", "\\'") if dim.threshold_desc else ""
            eval_method = dim.eval_method.replace("'", "\\'") if dim.eval_method else ""
            
            row = [
                eval_id,
                symbol,
                stock_name,
                datetime.now(),  # datetime object
                int(dim.dim_id),
                dim.dim_name,
                dim.dim_category,
                float(dim.score),
                float(dim.max_score),
                eval_method,
                raw_value,
                threshold_desc,
                source_names,
                source_urls,
                datetime.now(),  # datetime object
                remark,
            ]
            rows.append(row)
        
        if rows:
            try:
                self.client.insert(
                    'stock_eval.evaluation_details',
                    rows,
                    column_names=column_names
                )
            except Exception as e:
                print(f"Warning: Failed to save dimension scores: {e}")
    
    def get_latest_evaluation(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新的评估结果"""
        try:
            result = self.client.query(
                "SELECT * FROM stock_eval.evaluation_results WHERE symbol = :symbol ORDER BY eval_time DESC LIMIT 1",
                parameters={'symbol': symbol}
            )
            rows = result.result_rows
            if rows:
                cols = [col[0] for col in result.column_names]
                return dict(zip(cols, rows[0]))
        except Exception as e:
            print(f"Error: {e}")
        return None
    
    def get_evaluation_history(self, symbol: str, limit: int = 10) -> List[Dict[str, Any]]:
        """获取评估历史"""
        try:
            result = self.client.query(
                "SELECT * FROM stock_eval.evaluation_results WHERE symbol = :symbol ORDER BY eval_time DESC LIMIT :limit",
                parameters={'symbol': symbol, 'limit': limit}
            )
            cols = [col[0] for col in result.column_names]
            return [dict(zip(cols, row)) for row in result.result_rows]
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def get_dimension_details(self, eval_id: str) -> List[Dict[str, Any]]:
        """获取评估明细"""
        try:
            result = self.client.query(
                "SELECT * FROM stock_eval.evaluation_details WHERE eval_id = :eval_id ORDER BY dim_id",
                parameters={'eval_id': eval_id}
            )
            cols = [col[0] for col in result.column_names]
            return [dict(zip(cols, row)) for row in result.result_rows]
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def get_all_evaluations(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取所有评估结果"""
        try:
            result = self.client.query(
                "SELECT symbol, stock_name, eval_time, total_score, max_score, score_ratio, rating FROM stock_eval.evaluation_results ORDER BY eval_time DESC LIMIT :limit",
                parameters={'limit': limit}
            )
            cols = [col[0] for col in result.column_names]
            return [dict(zip(cols, row)) for row in result.result_rows]
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def get_evaluations_paged(self, page: int = 1, page_size: int = 20, symbol_filter: str = None) -> Dict[str, Any]:
        """分页获取评估结果"""
        offset = (page - 1) * page_size
        params = {'limit': page_size, 'offset': offset}
        
        where_clause = ""
        if symbol_filter:
            where_clause = "WHERE symbol = :symbol_filter"
            params['symbol_filter'] = symbol_filter
        
        try:
            # Get total count
            count_sql = f"SELECT count() as total FROM stock_eval.evaluation_results {where_clause}"
            count_result = self.client.query(count_sql, parameters=params)
            total = count_result.result_rows[0][0] if count_result.result_rows else 0
            
            # Get data
            data_sql = f"""
                SELECT symbol, stock_name, eval_time, total_score, max_score, score_ratio, rating 
                FROM stock_eval.evaluation_results 
                {where_clause}
                ORDER BY eval_time DESC 
                LIMIT :limit OFFSET :offset
            """
            result = self.client.query(data_sql, parameters=params)
            cols = [col[0] for col in result.column_names]
            data = [dict(zip(cols, row)) for row in result.result_rows]
            
            return {
                'total': total,
                'page': page,
                'page_size': page_size,
                'data': data
            }
        except Exception as e:
            print(f"Error: {e}")
            return {'total': 0, 'page': page, 'page_size': page_size, 'data': []}
    
    def close(self):
        """关闭连接"""
        if self.client:
            self.client.close()
