
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

/** 笔记本版本 **/
/*%LET product_dir = F:\Research\GIT_BACKUP\events;*/
/*%LET utils_dir = D:\Research\GIT_BACKUP\utils\SAS\修改版本; */
/** 台式机版本 **/
%LET product_dir = F:\Research\GIT_BACKUP\events;
%LET utils_dir = F:\Research\GIT_BACKUP\utils\SAS\修改版本; 


%LET input_dir = &product_dir.\input_data\employer_holding; 
%LET output_dir = &product_dir.\output_data\employer_holding;
LIBNAME product "&product_dir.\sasdata\employer_holding";

%INCLUDE "&utils_dir.\日期_通用函数.sas";
%INCLUDE "&utils_dir.\交易_通用函数.sas";
%INCLUDE "&utils_dir.\权重_通用函数.sas";
%INCLUDE "&utils_dir.\组合构建_通用函数.sas";
%INCLUDE "&utils_dir.\其他_通用函数.sas";
%INCLUDE "&utils_dir.\事件研究_通用函数.sas";

options validvarname=any; /* 支持中文变量名 */

/*** step0: 调用事件研究_配置文件 **/
%LET env_start_date = 1jan2014;
%INCLUDE "&utils_dir.\事件研究_配置文件.sas";


/*** step1: 读入原始数据(永久保存) **/
%read_from_excel(excel_path=&input_dir.\员工持股计划数据.xlsx,  output_table=product.raw_data, sheet_name = Sheet1$);

/** Step2: 调整到交易日 */
%adjust_date_modify(busday_table=busday , raw_table=product.raw_data ,colname=report_date,  output_table=raw_data, is_forward = 1)
DATA raw_data;
	SET raw_data;
	event_id = _N_;
RUN;

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
	WHERE stock_code IN
	(SELECT stock_code FROM event_table2)
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

/*PROC SQL;*/
/*	CREATE TABLE index_hqinfo_subset AS*/
/*	SELECT A.end_date, A.stock_code, */
/*		CASE WHEN b.bk = "主板" THEN C.index_000300*/
/*			WHEN b.bk = "中小企业板" THEN C.index_399101*/
/*			WHEN b.bk = "创业板" THEN C.index_399102*/
/*			ELSE . END AS price,*/
/*		CASE WHEN b.bk = "主板" THEN D.index_000300*/
/*			WHEN b.bk = "中小企业板" THEN D.index_399101*/
/*			WHEN b.bk = "创业板" THEN D.index_399102*/
/*			ELSE . END AS last_price*/
/*	FROM hqinfo A LEFT JOIN stock_info_table B*/
/*	ON A.stock_code = B.stock_code*/
/*	LEFT JOIN index_hqinfo_close C*/
/*	ON A.end_date = C.end_date*/
/*	LEFT JOIN index_hqinfo_preclose D*/
/*	ON A.end_date = D.end_date*/
/*	ORDER BY A.end_date, A.stock_code;*/
/*QUIT;*/

/** 基准要求：
(1) 主板，总市值>200亿：沪深300
(2) 主板，总市值<200亿：中证500
(3) 创业板：创业板综
(4) 中小板：中小板综

注意：因为同个股票可能属于不同的基准。如果按照事件日确定的市值来判断，基准的选择。会出现同个股票可能有不同的基准。
因此：如果没有重复事件，则以event_date时市值确定的为准。如果有重复则以最早event_date的结果为准。
**/

PROC SQL;
	CREATE TABLE size_table AS
	SELECT A.*, A.price*B.freeshare/100000000 AS fmv, A.price*B.a_share/100000000 AS size
	FROM stock_hqinfo_subset A LEFT JOIN tinysoft.fg_wind_freeshare B
	ON A.end_date = datepart(B.end_date) AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;

PROC SQL;
	CREATE TABLE size_table_event AS
	SELECT A.stock_code, B.size
	FROM event_table2 A LEFT JOIN size_table B
	ON A.stock_code = B.stock_code AND A.event_date = B.end_Date
	GROUP BY A.stock_code
	HAVING A.event_date = min(A.event_date)
	ORDER BY A.stock_code;
QUIT;


PROC SQL;
	CREATE TABLE index_hqinfo_subset AS
	SELECT A.end_date, A.stock_code, 
		CASE WHEN f.bk = "中小企业板" THEN C.index_399101
			 WHEN f.bk = "创业板" THEN C.index_399102
			 WHEN f.bk = "主板" AND b.size >200  THEN C.index_000300
			 WHEN f.bk = "主板" AND 0 < b.size AND b.size <= 200  THEN C.index_000905
			 ELSE . END AS price,
		CASE WHEN f.bk = "中小企业板" THEN D.index_399101
			 WHEN f.bk = "创业板" THEN D.index_399102 
			 WHEN f.bk = "主板" AND b.size >200  THEN D.index_000300
			 WHEN f.bk = "主板" AND 0 < b.size AND b.size <= 200  THEN D.index_000905
			 ELSE . END AS last_price
	FROM stock_hqinfo_subset A LEFT JOIN size_table_event B
	ON A.stock_code = B.stock_code
	LEFT JOIN stock_info_table F
	ON A.stock_code = F.stock_code
	LEFT JOIN index_hqinfo_close C
	ON A.end_date = C.end_date
	LEFT JOIN index_hqinfo_preclose D
	ON A.end_date = D.end_date
	ORDER BY A.end_date, A.stock_code;
QUIT;

/*DATA tt;*/
/*	SET index_hqinfo_subset;*/
/*	IF missing(price) OR missing(last_price);*/
/*RUN;*/


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

%cal_win_ret(eventName=&event_name., event_hq_table=&event_name._hq, buy_win=0,  
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

%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha,
		alpha_var=alpha, is_group=1, group_var=lock_month, is_output = 1, file_name=&output_dir./result.xls, sheet_name=lock_month);

/*************** 结束逐条语句测试： 事件研究框架 */


/*************** Step6: 模块化事件研究框架（正式使用,example) */
/** Step1: 准备属性表，用于之后分组分析使用 */
DATA attribute_table(keep = stock_code end_date lock_month is_structure);
	SET product.raw_data;
	end_date = adj_report_date;
RUN;

%MACRO event_module(event_name, event_table, group_var, alpha_var=accum_alpha,
			start_win=-60, end_win=60, buy_win=0, win_ahead=-10,
			file_name=&output_dir./result2.xls);
	%gen_no_overlapped_win(eventName=&event_name., event_table=&event_table., stock_hqinfo_table=stock_hqinfo_subset, 
		bm_hqinfo_table=index_hqinfo_subset, start_win=&start_win., end_win=&end_win.);
	%mark_event_win(event_win_table=&event_name._hq, stock_info_table=stock_info_table, market_table = market_table, 
		output_table = &event_name._hq);	
	%mark_stat(event_win_table = &event_name._hq, output_table = &event_name._hq_stat);
	%cal_win_ret(eventName=&event_name., event_hq_table=&event_name._hq, buy_win=&buy_win.,  
		filter_invalid_after=1, filter_invalid_before=1, 
		filter_limit_after = 1, filter_limit_before = 1);
	/** 事件前表现 */
	/*%append_ahead_effect(alpha_table=&eventName._alpha, win_ahead=&win_ahead., alpha_table_edit=&eventName._alpha);*/
	%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha);
	%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha,
		alpha_var=&alpha_var., is_group=1, group_var=&group_var., is_output = 1, file_name=&file_name., sheet_name=&group_var.);
	/** 将不同时间窗口下的特殊市场情况结果也一并输出 */
/*	%output_to_excel(excel_path=&&file_name., input_table=&event_name._hq_stat, sheet_name = win_stat);*/
	
%MEND event_module;

%event_module(event_name = hold, event_table=event_table2, group_var = is_structure, alpha_var = accum_ret);


/************************************** 复制中信paper结果 *****************************/
/** 将后期的alpha计算挪到外面 */
%LET event_name = hold;
%MACRO event_module_neat(event_name, event_table,
			start_win=-60, end_win=60, buy_win=0, win_ahead=-10);
	%gen_no_overlapped_win(eventName=&event_name., event_table=&event_table., stock_hqinfo_table=stock_hqinfo_subset, 
		bm_hqinfo_table=index_hqinfo_subset, start_win=&start_win., end_win=&end_win.);
	%mark_event_win(event_win_table=&event_name._hq, stock_info_table=stock_info_table, market_table = market_table, 
		output_table = &event_name._hq);	
	%mark_stat(event_win_table = &event_name._hq, output_table = &event_name._hq_stat);
	%cal_win_ret(eventName=&event_name., event_hq_table=&event_name._hq, buy_win=&buy_win.,  
		filter_invalid_after=1, filter_invalid_before=1, 
		filter_limit_after = 1, filter_limit_before = 1);
%MEND event_module_neat;

%event_module_neat(event_name = hold, event_table=event_table2);


%alpha_detail_output(eventName=hold, event_table=raw_data, alpha_table=&event_name._alpha, 
	output_win=30, is_output = 1, file_name=&output_dir./detail.xlsx, sheet_name=detail_30, group_var=);

/** 1: 按照企业性质分组 */
DATA attribute_table(keep = stock_code end_date type_g);
	SET raw_data;
	end_date = adj_report_date;
	IF index(company_type, "国有企业") >0 THEN type_g = 1;
	ELSE IF index(company_type, "民营企业") >0 THEN type_g = 2;
	ELSE type_g = 3;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT type_g, count(1) AS nobs
	FROM attribute_table
	GROUP BY type_g;
QUIT;

%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=type_g, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=type_g);


/** 3: 股票来源 */
DATA attribute_table(keep = stock_code end_date sres_g);
	SET raw_data;
	end_date = adj_report_date;
	IF index(s_resource, "二级市场购买") >0 THEN sres_g = 1;
	ELSE IF index(s_resource, "非公开发行") >0 THEN sres_g = 2;
	ELSE IF index(s_resource, "赠与") > 0 THEN sres_g = 3;
	ELSE IF index(s_resource, "不确定") >0 THEN sres_g = 4;
	ELSE sres_g = 5;
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT sres_g, count(1) AS nobs
	FROM attribute_table
	GROUP BY sres_g;
QUIT;
%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=sres_g, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=sres_g);


/** 4: 是否为结构化 */
DATA attribute_table(keep = stock_code end_date is_structure);
	SET raw_data;
	end_date = adj_report_date;
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT is_structure, count(1) AS nobs
	FROM attribute_table
	GROUP BY is_structure;
QUIT;
%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=is_structure, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=is_structure);


/** 4-1: 二级市场内，结构化是否更好 */
/** 二级市场购买，不是一个很好的指标。（当控制结构化与否后，与非二级市场购买的样本差异缩小） */
DATA attribute_table(keep = stock_code end_date buy_g);
	SET raw_data;
	end_date = adj_report_date;
	IF index(s_resource, "二级市场购买") >0 AND is_structure = 1 THEN buy_g = 1;
	ELSE IF index(s_resource, "二级市场购买") >0 AND is_structure = 0 THEN buy_g = 2;
	ELSE buy_g = 3;
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT buy_g, count(1) AS nobs
	FROM attribute_table
	GROUP BY buy_g;
QUIT;
%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=buy_g, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=buy_g);


/** 4-2: 民营企业，结构化是否更好 */
/** 民营企业与否，不属于很好的指标 （当控制结构化与否后，与非民营企业的样本差异缩小）*/
DATA attribute_table(keep = stock_code end_date type_struc_g);
	SET raw_data;
	end_date = adj_report_date;
	IF index(company_type, "民营企业") >0 AND is_structure = 1  THEN type_struc_g = 1;
	ELSE IF index(company_type, "民营企业") >0 AND is_structure = 0 THEN type_struc_g = 2;
	ELSE IF index(company_type, "国有企业") >0 THEN type_struc_g = 3;
	ELSE type_struc_g = 4;
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT type_struc_g, count(1) AS nobs
	FROM attribute_table
	GROUP BY type_struc_g;
QUIT;
%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=type_struc_g, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=type_struc_g);


/** 2: 按照参与人数分组 */
/** 原始数据中，有一些cover_num=0，但是senior_num!=0或者senior_num也等于0的样本 */
DATA attribute_table(keep = stock_code end_date cover_g);
	SET raw_data;
	end_date = adj_report_date;
	IF cover_num = 0 THEN cover_g = 0;
	ELSE IF cover_num < 50 THEN cover_g = 1;
	ELSE IF cover_num <100 THEN cover_g = 2;
	ELSE IF cover_num < 150 THEN cover_g = 3;
	ELSE IF cover_num < 300 THEN cover_g = 4;
	ELSE IF cover_num < 500 THEN cover_g = 5;
	ELSE IF cover_num < 1000 THEN cover_g = 6;
	ELSE IF cover_num < 1500 THEN cover_g = 7;
	ELSE IF cover_num < 5000 THEN cover_g = 8;
	ELSE cover_g = 9;
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT cover_g, count(1) AS nobs
	FROM attribute_table
	GROUP BY cover_g;
QUIT;

%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=cover_g, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=cover_g);

/** 2-4: 2个维度 */
DATA attribute_table(keep = stock_code end_date cover_struc_g4);
	SET raw_data;
	end_date = adj_report_date;
	IF 0 < cover_num < 50 AND is_structure = 0  THEN cover_struc_g4 = 1;
	ELSE IF is_structure = 0 THEN cover_struc_g4 = 2;
	ELSE IF is_structure = 1 AND 0 < cover_num < 50 THEN cover_struc_g4 = 3;
	ELSE cover_struc_g4 = 4; 
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT cover_struc_g4, count(1) AS nobs
	FROM attribute_table
	GROUP BY cover_struc_g4;
QUIT;
%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=cover_struc_g4, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=cover_struc_g4);

/** 2-5: 整理为2个样本（最终划分标准） */
/**
(1)	参与人数少于50人
(2)	有杠杆产品设计
**/

DATA attribute_table(keep = stock_code end_date final_g);
	SET raw_data;
	end_date = adj_report_date;
	IF is_structure = 1 OR 0 < cover_num < 50 THEN final_g = 1;
	ELSE final_g = 0;
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT final_g, count(1) AS nobs
	FROM attribute_table
	GROUP BY final_g;
QUIT;
%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=final_g, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=final_g);




/** 5: 按照高管分组(未完成) */
/** 按照是否有高管参与 */
DATA attribute_table(keep = stock_code end_date senior_g);
	SET raw_data;
	end_date = adj_report_date;
	IF cover_num = 0 AND senior_num = 0 THEN senior_g = 1;
	ELSE IF senior_num = 0 THEN senior_g = 2;
	ELSE senior_g = 3;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT buy_g, count(1) AS nobs
	FROM attribute_table
	GROUP BY buy_g;
QUIT;
%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=senior_g, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=senior_g);


/** 5-2: 2个维度 */
DATA attribute_table(keep = stock_code end_date senior_struc_g4);
	SET raw_data;
	end_date = adj_report_date;
	IF senior_num > 0  AND is_structure = 0  THEN seniro_struc_g4 = 1;
	ELSE IF is_structure = 0 THEN senior_struc_g4 = 2;
	ELSE IF is_structure = 1 AND senior_num > 0 THEN senior_struc_g4 = 3;
	ELSE senior_struc_g4 = 4; 
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT senior_struc_g4, count(1) AS nobs
	FROM attribute_table
	GROUP BY senior_struc_g4;
QUIT;
%attribute_to_event(alpha_table=&event_name._alpha, attribute_table=attribute_table, alpha_table_edit=&event_name._alpha2);
%alpha_collect_by_group(eventName=&event_name., alpha_table=&event_name._alpha2,
		alpha_var=accum_alpha, is_group=1, group_var=cover_struc_g4, is_output = 1, file_name=&output_dir./accum_alpha.xlsx, sheet_name=cover_struc_g4);


/************************************** 构建组合 *****************************/
/** 2-5:组合构成
/**
(1)	参与人数少于50人
(2)	有杠杆产品设计。

持有时间：35个交易日。
**/

DATA attribute_table(keep = stock_code end_date final_g);
	SET raw_data;
	end_date = adj_report_date;
	IF is_structure = 1 OR 0 < cover_num < 50 THEN final_g = 1;
	ELSE final_g = 0;
RUN;
PROC SQL;
	CREATE TABLE subset AS
	SELECT A.stock_code, A.event_date, A.event_id
	FROM event_table2 A LEFT JOIN attribute_table B
	ON A.stock_code = B.stock_code AND A.event_date = B.end_date
	WHERE b.final_g = 1;
QUIT;

%gen_no_overlapped_win(eventName=subset, event_table=subset, stock_hqinfo_table=stock_hqinfo_subset, 
		bm_hqinfo_table=index_hqinfo_subset, start_win=1, end_win=35);
