-- =====================================================
-- A股基本面评估系统 - ClickHouse 数据库初始化
-- =====================================================

-- 创建评估数据库
CREATE DATABASE IF NOT EXISTS stock_eval;

-- =====================================================
-- 1. 评估结果表（每次评估的汇总结果）
-- =====================================================
CREATE TABLE IF NOT EXISTS stock_eval.evaluation_results
(
    -- 评估ID
    eval_id String UUID DEFAULT generateUUIDv4(),
    
    -- 股票信息
    symbol String COMMENT '股票代码，如 sh.600000',
    stock_name String COMMENT '股票名称',
    
    -- 评估时间
    eval_time DateTime DEFAULT now(),
    eval_date Date DEFAULT toDate(now()),
    
    -- 总评分
    total_score Float64 COMMENT '总分，满分120',
    max_score Float64 DEFAULT 120 COMMENT '满分',
    score_ratio Float64 COMMENT '得分率，总分/满分',
    
    -- 评级
    rating String COMMENT '评级：优秀/良好/中等/较差/差',
    
    -- 各维度得分汇总
    score_信息披露 Float64 COMMENT '维度1-3合计',
    score_股权结构 Float64 COMMENT '维度4-6合计',
    score_业务清晰度 Float64 COMMENT '维度7合计',
    score_财务质量 Float64 COMMENT '维度8-10合计',
    score_股东行为 Float64 COMMENT '维度11-12合计',
    score_监管法律 Float64 COMMENT '维度13-14合计',
    score_行业景气 Float64 COMMENT '维度15-16合计',
    score_生产经营 Float64 COMMENT '维度17-18合计',
    score_技术面 Float64 COMMENT '维度19-20合计',
    score_外部环境 Float64 COMMENT '维度21-23合计',
    
    -- 元数据
    data_source String COMMENT '数据来源版本',
    remark String COMMENT '备注'
)
ENGINE = MergeTree()
PARTITION BY (eval_date, stock_name)
ORDER BY (symbol, eval_time)
TTL eval_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- =====================================================
-- 2. 评估明细表（每项维度的详细评分）
-- =====================================================
CREATE TABLE IF NOT EXISTS stock_eval.evaluation_details
(
    -- 关联评估ID
    eval_id String COMMENT '关联evaluation_results.eval_id',
    
    -- 股票信息
    symbol String COMMENT '股票代码',
    stock_name String COMMENT '股票名称',
    eval_time DateTime DEFAULT now(),
    
    -- 维度信息
    dim_id UInt8 COMMENT '维度ID 1-24',
    dim_name String COMMENT '维度名称',
    dim_category String COMMENT '维度类别',
    
    -- 评分
    score Float64 COMMENT '得分',
    max_score Float64 DEFAULT 5 COMMENT '满分',
    
    -- 评分依据
    eval_method String COMMENT '评估方法',
    raw_value String COMMENT '原始数据值',
    threshold_desc String COMMENT '评分阈值说明',
    
    -- 数据来源
    data_sources String COMMENT '数据源，多个用逗号分隔',
    source_urls String COMMENT '来源URL，多个用换行分隔',
    fetch_time DateTime DEFAULT now() COMMENT '数据获取时间',
    
    -- 备注
    remark String COMMENT '额外说明'
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(eval_time), dim_category)
ORDER BY (symbol, eval_time, dim_id)
TTL eval_time + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- =====================================================
-- 3. 数据获取日志表（记录每次数据抓取）
-- =====================================================
CREATE TABLE IF NOT EXISTS stock_eval.data_fetch_log
(
    fetch_id String UUID DEFAULT generateUUIDv4(),
    fetch_time DateTime DEFAULT now(),
    
    -- 股票信息
    symbol String COMMENT '股票代码',
    stock_name String COMMENT '股票名称',
    
    -- 数据源
    source_name String COMMENT '数据源名称，如 eastmoney',
    source_type String COMMENT '数据类型，如 研报, 公告, 董秘问答',
    
    -- 请求信息
    request_url String COMMENT '请求URL',
    request_params String COMMENT '请求参数',
    
    -- 响应信息
    response_code UInt16 COMMENT 'HTTP响应码',
    response_size UInt32 COMMENT '响应大小',
    data_count UInt32 COMMENT '获取数据条数',
    
    -- 状态
    status String COMMENT 'success/failed/partial',
    error_msg String COMMENT '错误信息',
    duration_ms UInt32 COMMENT '耗时毫秒'
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(fetch_time), source_name)
ORDER BY (symbol, fetch_time)
TTL fetch_time + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

-- =====================================================
-- 4. 原始数据缓存表（存储抓取的原始数据）
-- =====================================================
CREATE TABLE IF NOT EXISTS stock_eval.raw_data_cache
(
    cache_key String COMMENT '缓存Key：symbol_source_type_date',
    symbol String COMMENT '股票代码',
    source_name String COMMENT '数据源',
    data_type String COMMENT '数据类型',
    fetch_date Date DEFAULT toDate(now()),
    fetch_time DateTime DEFAULT now(),
    
    -- 原始数据
    raw_json String COMMENT '原始JSON数据',
    raw_count UInt32 COMMENT '数据条数',
    
    -- 有效期
    expire_time DateTime COMMENT '过期时间',
    is_valid UInt8 DEFAULT 1 COMMENT '是否有效'
)
ENGINE = MergeTree()
PARTITION BY (source_name, data_type)
ORDER BY (symbol, fetch_time)
TTL expire_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- =====================================================
-- 5. 评估历史对比表（方便查询历史评估）
-- =====================================================
CREATE TABLE IF NOT EXISTS stock_eval.evaluation_history
(
    symbol String COMMENT '股票代码',
    stock_name String COMMENT '股票名称',
    eval_date Date COMMENT '评估日期',
    total_score Float64 COMMENT '当日总分',
    rating String COMMENT '当日评级',
    
    -- 趋势标记
    score_change Float64 COMMENT '与上次评估相比变化',
    rating_change String COMMENT '评级变化'
)
ENGINE = ReplacingMergeTree(eval_date)
PARTITION BY symbol
ORDER BY (symbol, eval_date)
SETTINGS index_granularity = 8192;

-- =====================================================
-- 创建视图：最新评估结果
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS stock_eval.latest_evaluation
ENGINE = MergeTree()
PARTITION BY symbol
ORDER BY (symbol, eval_time)
AS SELECT
    symbol,
    stock_name,
    eval_time,
    total_score,
    rating,
    score_信息披露,
    score_股权结构,
    score_财务质量,
    score_监管法律
FROM stock_eval.evaluation_results;

-- =====================================================
-- 初始化完成
-- =====================================================
SELECT 'stock_eval database initialized successfully' AS status;
