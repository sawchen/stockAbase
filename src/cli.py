#!/usr/bin/env python3
"""
命令行入口 - A股基本面量化评估系统
"""

import argparse
import sys
import yaml
from datetime import datetime

from evaluator import StockEvaluator
from database import StockEvalDB


def load_config():
    """加载配置"""
    with open('config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def format_report(result):
    """格式化评估报告"""
    lines = []
    lines.append("=" * 60)
    lines.append(f"A股基本面评估报告")
    lines.append("=" * 60)
    lines.append(f"股票代码: {result.symbol}")
    lines.append(f"股票名称: {result.stock_name}")
    lines.append(f"评估时间: {result.eval_time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append(f"【总分】{result.total_score:.1f}/120 ({result.score_ratio*100:.1f}%)")
    lines.append(f"【评级】{result.rating.value if hasattr(result.rating, 'value') else result.rating}")
    lines.append("")
    
    lines.append("-" * 60)
    lines.append("【各类别得分】")
    lines.append(f"  信息披露: {result.score_信息披露:.1f}/15")
    lines.append(f"  股权结构: {result.score_股权结构:.1f}/15")
    lines.append(f"  业务清晰度: {result.score_业务清晰度:.1f}/5")
    lines.append(f"  财务质量: {result.score_财务质量:.1f}/15")
    lines.append(f"  股东行为: {result.score_股东行为:.1f}/10")
    lines.append(f"  监管法律: {result.score_监管法律:.1f}/10")
    lines.append(f"  行业景气: {result.score_行业景气:.1f}/10")
    lines.append(f"  生产经营: {result.score_生产经营:.1f}/10")
    lines.append(f"  技术面: {result.score_技术面:.1f}/10")
    lines.append(f"  外部环境: {result.score_外部环境:.1f}/15")
    
    lines.append("")
    lines.append("-" * 60)
    lines.append("【维度明细】")
    
    for dim in result.dimensions:
        lines.append(f"\n{dim.dim_id:2d}. {dim.dim_name} ({dim.score:.1f}/{dim.max_score}分)")
        lines.append(f"    原始数据: {dim.raw_value}")
        lines.append(f"    评分说明: {dim.threshold_desc}")
        lines.append(f"    评估方法: {dim.eval_method}")
        
        if dim.data_sources:
            lines.append("    数据来源:")
            for ds in dim.data_sources:
                lines.append(f"      - [{ds.name}] {ds.url}")
        
        if dim.remark:
            lines.append(f"    备注: {dim.remark}")
    
    lines.append("")
    lines.append("=" * 60)
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='A股基本面量化评估系统')
    parser.add_argument('--symbol', required=True, help='股票代码，如 sh.600000')
    parser.add_argument('--name', help='股票名称（可选）')
    parser.add_argument('--save', action='store_true', help='保存到数据库')
    parser.add_argument('--config', default='config.yaml', help='配置文件路径')
    
    args = parser.parse_args()
    
    # 加载配置
    config = load_config()
    
    # 创建评估器
    evaluator = StockEvaluator()
    
    print(f"正在评估 {args.symbol} ...", file=sys.stderr)
    
    # 执行评估
    result = evaluator.evaluate(args.symbol, args.name)
    
    # 输出报告
    print(format_report(result))
    
    # 保存到数据库
    if args.save:
        try:
            db_config = config['clickhouse']
            db = StockEvalDB(
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=db_config['password'],
                database=config.get('eval_database', 'stock_eval')
            )
            
            eval_id = db.save_evaluation_result(result)
            db.save_dimension_scores(eval_id, result.dimensions, result.symbol, result.stock_name)
            db.close()
            
            print(f"\n✅ 已保存到数据库 (eval_id: {eval_id})", file=sys.stderr)
        except Exception as e:
            print(f"\n⚠️ 保存失败: {e}", file=sys.stderr)


if __name__ == '__main__':
    main()
