"""
数据模型 - 定义评估系统的数据结构
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class Rating(Enum):
    """评级枚举"""
    EXCELLENT = "优秀"      # 得分率 >= 90%
    GOOD = "良好"           # 得分率 >= 75%
    MEDIUM = "中等"         # 得分率 >= 60%
    FAIR = "较差"          # 得分率 >= 40%
    POOR = "差"            # 得分率 < 40%


@dataclass
class DataSource:
    """数据源"""
    name: str                    # 数据源名称
    url: str                     # 数据URL
    fetch_time: datetime         # 获取时间
    raw_data: Any = None         # 原始数据
    status: str = "success"     # 状态


@dataclass
class DimensionScore:
    """维度评分"""
    dim_id: int                  # 维度ID 1-24
    dim_name: str                # 维度名称
    dim_category: str            # 所属类别
    score: float                 # 得分
    max_score: float = 5.0       # 满分
    eval_method: str = ""        # 评估方法
    raw_value: str = ""          # 原始数据值
    threshold_desc: str = ""     # 阈值说明
    data_sources: List[DataSource] = field(default_factory=list)
    remark: str = ""             # 备注

    @property
    def score_ratio(self) -> float:
        return self.score / self.max_score if self.max_score > 0 else 0


@dataclass
class EvaluationResult:
    """评估结果"""
    eval_id: str                 # 评估ID
    symbol: str                  # 股票代码
    stock_name: str              # 股票名称
    eval_time: datetime          # 评估时间
    
    # 总分
    total_score: float           # 总得分
    max_total_score: float = 120.0  # 满分
    
    # 评级
    rating: Rating = Rating.MEDIUM
    
    # 各类别得分
    score_信息披露: float = 0.0
    score_股权结构: float = 0.0
    score_业务清晰度: float = 0.0
    score_财务质量: float = 0.0
    score_股东行为: float = 0.0
    score_监管法律: float = 0.0
    score_行业景气: float = 0.0
    score_生产经营: float = 0.0
    score_技术面: float = 0.0
    score_外部环境: float = 0.0
    
    # 维度明细
    dimensions: List[DimensionScore] = field(default_factory=list)
    
    # 数据来源
    data_sources: List[DataSource] = field(default_factory=list)
    
    remark: str = ""             # 备注

    @property
    def score_ratio(self) -> float:
        return self.total_score / self.max_total_score if self.max_total_score > 0 else 0
    
    def get_dimension(self, dim_id: int) -> Optional[DimensionScore]:
        for dim in self.dimensions:
            if dim.dim_id == dim_id:
                return dim
        return None


# 维度分类映射
DIMENSION_CATEGORIES = {
    1: ("信息披露", "资讯更新频率"),
    2: ("信息披露", "董秘互动质量"),
    3: ("信息披露", "研报覆盖频率"),
    4: ("股权结构", "实控人持股比例"),
    5: ("股权结构", "董事长持股与年龄"),
    6: ("股权结构", "机构持仓"),
    7: ("业务清晰度", "主营业务清晰度"),
    8: ("财务质量", "盈利稳定性"),
    9: ("财务质量", "资产负债率"),
    10: ("财务质量", "融资比例"),
    11: ("股东行为", "减持行为"),
    12: ("股东行为", "股东人数变化"),
    13: ("监管法律", "监管立案"),
    14: ("监管法律", "法律诉讼"),
    15: ("行业景气", "行业趋势"),
    16: ("行业景气", "业绩支撑逻辑"),
    17: ("生产经营", "供应链风险"),
    18: ("生产经营", "产品竞争力"),
    19: ("技术面", "均线趋势"),
    20: ("技术面", "MACD趋势"),
    21: ("外部环境", "年报季报风险"),
    22: ("外部环境", "投行评级"),
    23: ("外部环境", "政策舆论"),
    24: ("外部环境", "中美关系冲击"),
}


def get_rating(score_ratio: float) -> Rating:
    """根据得分率返回评级"""
    if score_ratio >= 0.90:
        return Rating.EXCELLENT
    elif score_ratio >= 0.75:
        return Rating.GOOD
    elif score_ratio >= 0.60:
        return Rating.MEDIUM
    elif score_ratio >= 0.40:
        return Rating.FAIR
    else:
        return Rating.POOR


def format_dimension_report(dim: DimensionScore) -> str:
    """格式化维度评估报告"""
    sources = "\n".join([f"- [{s.name}]({s.url})" for s in dim.data_sources]) if dim.data_sources else "无"
    
    return f"""### {dim.dim_id}. {dim.dim_name} ({dim.score}/{dim.max_score}分)

**评估方法**: {dim.eval_method}

**原始数据**: {dim.raw_value}

**评分说明**: {dim.threshold_desc}

**数据来源**:
{sources}

**备注**: {dim.remark if dim.remark else "无"}
"""
