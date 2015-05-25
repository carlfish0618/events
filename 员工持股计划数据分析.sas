/*** 员工持股计划数据分析 **/
/** 数据字段包括：
(1) have_condition: 是否有授予条件
(2) report_date: 公告日期
(3) cover_num: 覆盖人数
(4) senior_num: 高管人数
(5) money: 金额（万元）
(6) lock_month: 锁定期
(7) c_resource: 资金来源
(8) s_resource: 股票来源
(9) is_structure: 是否结构化
********/


%LET product_dir = D:\Research\Data\员工持股计划\员工持股计划;
%LET input_dir = &product_dir.\input_data\employer_holding; 
%LET output_dir = &product_dir.\output_data\employer_holding;
LIBNAME product "&product_dir.\sasdata\employer_holding";

%LET utils_dir = D:\Research\Data\员工持股计划\员工持股计划\修改版本; 
%INCLUDE "&utils_dir.\日期_通用函数.sas";
%INCLUDE "&utils_dir.\交易_通用函数.sas";
%INCLUDE "&utils_dir.\权重_通用函数.sas";
%INCLUDE "&utils_dir.\组合构建_通用函数.sas";
%INCLUDE "&utils_dir.\其他_通用函数.sas";
%INCLUDE "&utils_dir.\事件研究_通用函数.sas";

options validvarname=any; /* 支持中文变量名 */

/*** step1: 读入原始数据(永久保存) **/
%read_from_excel(excel_path=&input_dir.\员工持股计划数据.xlsx,  output_table=product.raw_data, sheet_name = Sheet1$);

/** Step2: 调整到交易日 */
%adjust_date_modify(busday_table=busday , raw_table=product.raw_data ,colname=report_date,  output_table=raw_data, is_forward = 1 )

/** 统计：周末发布的样本数 */
/** 21的样本在周末发布消息。97个在交易日发布消息 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT report_date_is_busday, count(1) AS nobs
	FROM raw_data
	GROUP BY report_date_is_busday;
QUIT;


/** Step3: 提取子集构建事件表 */
DATA event_table(keep = stock_code event_id event_date);
	SET raw_data;
	event_id = _N_;
	event_date = adj_report_date;
	FORMAT event_date yymmdd10.;
RUN;
PROC SORT DATA = event_table NODUPKEY;
	BY event_date stock_code;
RUN;


/** Step4: 过滤部分事件 */
%filter_event(event_table, stock_info_table, market_table, event_table2, ndays = 365, halt_days = 1, is_filter_mark = 1);

/* 筛选出被过滤的事件 */
/** 300376：上市时间未满1年 */
/** 有6只股票，发布当天停牌。其中3只停牌超过20个交易日（3只中有一只停牌超过60个交易日) */
DATA filter_event;
	SET event_table2;
	IF filter >0;
RUN;

/** 最终：只剔除上市未满1年的股票 **/
%filter_event(event_table, stock_info_table, market_table, event_table2, ndays = 365, halt_days = 10000, is_filter_mark = 0);

/** Step5: 取个股和基准价格数据，用于之后事件分析中使用 ***/
/** 个股价格数据 */
PROC SQL;
	CREATE TABLE stock_hqinfo_subset AS
	SELECT end_date, 
	stock_code, close AS price LABEL "price", 
	pre_close AS last_price LABEL "last_price"
	FROM hqinfo
	ORDER BY end_date, stock_code;
QUIT;

/* 基准价格数据（每个股票可以有各自的基准）*/
PROC TRANSPOSE DATA = index_hqinfo OUT = index_hqinfo_close(drop = _NAME_ _LABEL_) prefix = index_;
	ID stock_code;
	VAR close;
	BY end_date;
RUN;

PROC TRANSPOSE DATA = index_hqinfo OUT = index_hqinfo_preclose(drop = _NAME_ _LABEL_) prefix = index_;
	ID stock_code;
	VAR pre_close;
	BY end_date;
RUN;

PROC SQL;
	CREATE TABLE index_hqinfo_subset AS
	SELECT A.end_date, A.stock_code, 
		CASE WHEN b.bk = "主板" THEN C.index_000300
			WHEN b.bk = "中小企业板" THEN C.index_399101
			WHEN b.bk = "创业板" THEN C.index_399102
			ELSE . END AS price,
		CASE WHEN b.bk = "主板" THEN D.index_000300
			WHEN b.bk = "中小企业板" THEN D.index_399101
			WHEN b.bk = "创业板" THEN D.index_399102
			ELSE . END AS last_price
	FROM hqinfo A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code
	LEFT JOIN index_hqinfo_close C
	ON A.end_date = C.end_date
	LEFT JOIN index_hqinfo_preclose D
	ON A.end_date = D.end_date
	ORDER BY A.end_date, A.stock_code;
QUIT;


/*************** Step6-test: 逐条语句测试： 事件研究框架（可略过该步骤，直接采用之后的模块化测试) */

%LET event_name = hold;
/*%gen_overlapped_win(eventName=hold, event_table=event_table2, stock_hqinfo_table=stock_hqinfo_subset, */
/*	bm_hqinfo_table=index_hqinfo_subset, start_win=-60, end_win=60, busday_table = busday);*/

%gen_no_overlapped_win(eventName=&event_name., event_table=event_table2, stock_hqinfo_table=stock_hqinfo_subset, 
	bm_hqinfo_table=index_hqinfo_subset, start_win=-60, end_win=60);

%mark_event_win(event_win_table=&event_name._hq, stock_info_table=stock_info_table, market_table = market_table, 
	output_table = &event_name._hq);	
/* 不同时间窗口下，特殊市场情况的分布 */
%mark_stat(event_win_table = &event_name._hq, output_table = &event_name._hq_stat);

/** 特殊市场情况的分布 */
/** 878个样本属于停牌。27个属于一字涨停的。22个属于非复牌日，一字涨停的 */
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT mark, count(1) AS nobs*/
/*	FROM &event_name._hq*/
/*	GROUP BY mark;*/
/*QUIT;*/

%cal_win_ret(eventName=hold, event_hq_table=product.hold_hq, buy_win=0,  
		filter_invalid_after=1, filter_invalid_before=1, 
		filter_limit_after = 1, filter_limit_before = 1);

/** 检查是否有alpha或者ret缺失的情况 */
/* 不存在这种情况 */
/*DATA tt;*/
/*	SET &eventName._alpha;*/
/*	IF test_mark = 0;*/
/*RUN;*/

/** 检查filter_invalid/filter_limit的影响是否正确 */
/*DATA tt;*/
/*	SET &eventName._alpha;*/
/*	IF mark =1;*/
/*RUN;*/
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT win, sum(not missing(alpha_mdf)) AS nobs,*/
/*		count(1) AS nobs_f*/
/*	FROM &eventName._alpha*/
/*	GROUP BY win;*/
/*QUIT;*/

/** 事件前表现 */
/*%append_ahead_effect(alpha_table=&eventName._alpha, win_ahead=-10, alpha_table_edit=&eventName._alpha2);*/
DATA attribute_table(keep = stock_code end_date lock_month is_structure);
	SET product.raw_data;
	end_date = adj_report_date;
RUN;

%attribute_to_event(alpha_table=&eventName._alpha, attribute_table=attribute_table, alpha_table_edit=&eventName._alpha);
%alpha_collect_by_group(eventName=&eventName., alpha_table=&eventName._alpha,
		alpha_var=alpha, is_group=1, group_var=lock_month, is_output = 1, file_name=&output_dir./result.xls, sheet_name=lock_month);

/*************** 结束逐条语句测试： 事件研究框架 */


/*************** Step6: 模块化事件研究框架（正式使用) */
/** Step1: 准备属性表，用于之后分组分析使用 */
DATA attribute_table(keep = stock_code end_date lock_month is_structure);
	SET product.raw_data;
	end_date = adj_report_date;
RUN;

%MACRO event_module(event_name, event_table, group_var, alpha_var=accum_alpha, start_win=-60, end_win=60, buy_win=0, win_ahead=-10);
	%gen_no_overlapped_win(eventName=&event_name., event_table=&event_table., stock_hqinfo_table=stock_hqinfo_subset, 
		bm_hqinfo_table=index_hqinfo_subset, start_win=&start_win., end_win=&end_win.);
	%mark_event_win(event_win_table=&event_name._hq, stock_info_table=stock_info_table, market_table = market_table, 
		output_table = &event_name._hq);	
	%mark_stat(event_win_table = &event_name._hq, output_table = &event_name._hq_stat);
	%cal_win_ret(eventName=hold, event_hq_table=product.hold_hq, buy_win=&buy_win.,  
		filter_invalid_after=1, filter_invalid_before=1, 
		filter_limit_after = 1, filter_limit_before = 1);
	/** 事件前表现 */
	/*%append_ahead_effect(alpha_table=&eventName._alpha, win_ahead=&win_ahead., alpha_table_edit=&eventName._alpha);*/
	%attribute_to_event(alpha_table=&eventName._alpha, attribute_table=attribute_table, alpha_table_edit=&eventName._alpha);
	%alpha_collect_by_group(eventName=&eventName., alpha_table=&eventName._alpha,
		alpha_var=&alpha_var., is_group=1, group_var=&group_var., is_output = 1, file_name=&output_dir./result.xls, sheet_name=&group_var.);
	/** 将不同时间窗口下的特殊市场情况结果也一并输出 */
	%output_to_excel(excel_path=&output_dir./result.xls, input_table=&event_name._hq_stat, sheet_name = win_stat);
	
%MEND event_module;

%event_module(event_name = hold, event_table=event_table2, group_var = is_structure, alpha_var = accum_alpha);


/************************************** 复制中信paper结果 *****************************/
/** 1: 按照参与人数分组 */
/** 原始数据中，有一些cover_num=0，但是senior_num!=0或者senior_num也等于0的样本 */
DATA attribute_table(keep = stock_code end_date cover_g);
	SET product.raw_data;
	end_date = adj_report_date;
	IF cover_num < 50 THEN cover_g = 1;
	ELSE IF cover_num >= 50 AND cover_num <100 THEN cover_g = 2;
	ELSE IF cover_num >= 100 AND cover_num < 150 THEN cover_g = 3;
	ELSE IF cover_num >= 150 AND cover_num < 300 THEN cover_g = 4;
	ELSE IF cover_num >= 300 AND cover_num < 500 THEN cover_g = 5;
	ELSE IF cover_num >= 500 AND cover_num < 1000 THEN cover_g = 6;
	ELSE IF cover_num >= 1000 AND cover_num < 1500 THEN cover_g = 7;
	ELSE IF cover_num >= 1500 AND cover_num < 5000 THEN cover_g = 8;
	ELSE cover_g = 9;
RUN;
%event_module(event_name = hold, event_table=event_table2, group_var = cover_g, alpha_var = accum_alpha);

/** 1: 按照参与人数分组 */
/** 按照是否有高管参与 */
DATA attribute_table(keep = stock_code end_date senior_g);
	SET product.raw_data;
	end_date = adj_report_date;
	IF cover_num = 0 AND senior_num = 0 THEN senior_g = 1;
	ELSE IF senior_num = 0 THEN senior_g = 2;
	ELSE senior_g = 3;
RUN;
%event_module(event_name = hold, event_table=event_table2, group_var = senior_g, alpha_var = accum_alpha);


