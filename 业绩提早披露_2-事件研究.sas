

/*************** Step1A: 事件研究：不同组从预案披露日期到正式财报发布日之间的收益 */

%LET event_start_date = 31dec2005;
%LET event_end_date = 31oct2015;


/* 将actdate调整到交易日 */
%adjust_date_to_mapdate(rawdate_table=yypl_with_earning, mapdate_table=busday, raw_colname=actdate, 
		map_colname=date, output_table=yypl_s,is_backward=0, is_included=1);

/** 定义map_date1提前一天 */
%adjust_date_to_mapdate(rawdate_table=yypl_s, mapdate_table=busday, raw_colname=map_date1, 
		map_colname=date, output_table=yypl_s,is_backward=1, is_included=0);

/* 进入日：pub_date(即每年1月1日) */
/* 退出日：正式财报日 */
PROC SQL;
	CREATE TABLE cross_sample AS
	SELECT stock_code AS stock_code LABEL "stock_code",
		report_period,
		pub_date AS event_date,
		intval_group,
		intval,
		intval_days,
		min(map_map_date1, map_actdate) AS date_end FORMAT yymmdd10.
	FROM yypl_s
	WHERE not missing(pub_date) AND "&event_start_date."d <= pub_date <= "&event_end_date."d
	AND not missing(map_actdate)
	ORDER BY stock_code, report_period, pub_date;
QUIT;
PROC SORT DATA = cross_sample NODUPKEY OUT = cross_sample;
	BY stock_code report_period;
RUN;


DATA cross_sample;
	SET cross_sample;
	event_id = _N_;
RUN;


/*************** Step1B: 事件研究：只选择那些未发布业绩预告的事件 */
%LET event_start_date = 31dec2005;
%LET event_end_date = 31oct2015;

/*** 只选择那些未发布业绩预告的样本 */
PROC SQL;
	CREATe TABLE yypl_s AS
	SELECT *
	FROM yypl_with_earning2
	WHERE event_id NOT IN (
		SELECT event_id 
		FROM yypl_with_earning2
		WHERE map_f_date<= pub_date AND have_forecast=1);
QUIT;
/* 以第一条记录为准(想考虑是否合适作为退出时点)*/
PROC SORT DATA = yypl_s OUT = yypl_s;
	BY event_id f_date descending source;  
RUN;

PROC SORT DATA = yypl_s NODUPKEY OUT = yypl_s;
	BY event_id;
RUN;

/* 将actdate调整到交易日 */
%adjust_date_to_mapdate(rawdate_table=yypl_s, mapdate_table=busday, raw_colname=actdate, 
		map_colname=date, output_table=yypl_s,is_backward=0, is_included=1);

/** 定义map_date1提前一天 */
%adjust_date_to_mapdate(rawdate_table=yypl_s, mapdate_table=busday, raw_colname=map_date1, 
		map_colname=date, output_table=yypl_s,is_backward=1, is_included=0);

/* 进入日：pub_date(即每年1月1日) */
/* 退出日：min(正式财报日，预约日，业绩预告日) */
PROC SQL;
	CREATE TABLE cross_sample AS
	SELECT stock_code AS stock_code LABEL "stock_code",
		report_period,
		pub_date AS event_date,
		intval_group,
		intval,
		intval_days,
		min(map_map_date1, map_actdate, map_f_date) AS date_end FORMAT yymmdd10.,
		have_forecast
	FROM yypl_s
	WHERE not missing(pub_date) AND "&event_start_date."d <= pub_date <= "&event_end_date."d
	AND not missing(map_actdate)
	ORDER BY stock_code, report_period, pub_date;
QUIT;
PROC SORT DATA = cross_sample NODUPKEY OUT = cross_sample;
	BY stock_code report_period;
RUN;


DATA cross_sample;
	SET cross_sample;
	event_id = _N_;
RUN;



/*************** Step1C: 事件研究：选择那些已经发布业绩预告且为负面或不确定的事件 */
%LET event_start_date = 31dec2005;
%LET event_end_date = 31oct2015;

/*** 只选择那些未发布业绩预告的样本 */
PROC SQL;
	CREATe TABLE yypl_s AS
	SELECT *
	FROM yypl_with_earning2
	WHERE have_forecast = 1 AND map_f_date <= pub_date;
QUIT;
/* 以最后一条为准*/
PROC SORT DATA = yypl_s OUT = yypl_s;
	BY event_id descending f_date descending source;  
RUN;
PROC SORT DATA = yypl_s NODUPKEY OUT = yypl_s;
	BY event_id;
RUN;
DATA yypl_s;
	SET yypl_s;
/*	IF e_type IN ("预增","预盈","预升");*/
/*	IF e_type IN ("预减","预降","预亏");*/
/*	IF e_type IN ("预盈","预升");*/
/*    IF e_type NOT IN ("预增","预减","预升","预降", "预亏","预盈");*/
	IF e_type IN ("预盈");
RUN;





/* 将actdate调整到交易日 */
%adjust_date_to_mapdate(rawdate_table=yypl_s, mapdate_table=busday, raw_colname=actdate, 
		map_colname=date, output_table=yypl_s,is_backward=0, is_included=1);

/** 定义map_date1提前一天 */
%adjust_date_to_mapdate(rawdate_table=yypl_s, mapdate_table=busday, raw_colname=map_date1, 
		map_colname=date, output_table=yypl_s,is_backward=1, is_included=0);

/* 进入日：pub_date(即每年1月1日) */
/* 退出日：min(正式财报日，预约日，业绩预告日) */
PROC SQL;
	CREATE TABLE cross_sample AS
	SELECT stock_code AS stock_code LABEL "stock_code",
		report_period,
		pub_date AS event_date,
		intval_group,
		intval,
		intval_days,
		min(map_map_date1, map_actdate) AS date_end FORMAT yymmdd10.,
		have_forecast
	FROM yypl_s
	WHERE not missing(pub_date) AND "&event_start_date."d <= pub_date <= "&event_end_date."d
	AND not missing(map_actdate)
	ORDER BY stock_code, report_period, pub_date;
QUIT;
PROC SORT DATA = cross_sample NODUPKEY OUT = cross_sample;
	BY stock_code report_period;
RUN;


DATA cross_sample;
	SET cross_sample;
	event_id = _N_;
RUN;

/************************************** 以下适合所有的事件研究 ***********************************/

%event_gen_fixed_windows(event_table=cross_sample, start_col=event_date, end_col=date_end, busday_table = busday, output_table=cross_win);
/** 统计时间跨度分布 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT max_win, count(1) AS nobs
	FROM(
		SELECT event_id, 
			max(win) AS max_win,
			min(win) AS min_win
		FROM cross_win
		GROUP BY event_id
	)
	GROUP BY max_win;
QUIT;

PROC SQL;
	CREATE TABLE stat AS
	SELECT intval_group, count(1) AS nobs,
			max(win) AS max_win
	FROM cross_win
	GROUP BY intval_group;
QUIT;
%event_get_marketdata(win_table=cross_win, buy_win=0, hq_table=hqinfo, bm_table=bm_hqinfo, busday_table=busday, output_table=cross_rtn);
%event_mark_win(rtn_table=cross_rtn, stock_info_table=stock_info_table, output_table=cross_rtn);
/** 分析filter分布 */
PROC SQL;
	CREATE TABLE fh_filter_stat AS
	SELECT win, filter, count(1) AS nobs
	FROM cross_rtn
	GROUP BY win, filter;
QUIT;
PROC TRANSPOSE DATA = fh_filter_stat OUT = fh_filter_stat;
	BY win;
	ID filter;
RUN;

%event_cal_accum_alpha(rtn_table=cross_rtn,buy_win=0, output_table=cross_rtn);
%event_smooth_rtn(rtn_table=cross_rtn, buy_win=0, suffix=sm, output_table=cross_rtn, filter_set=(1,2,3,4,5));

/** 统计指标 */
/*%event_cal_stat(rtn_table=cross_rtn, rtn_var=accum_alpha_sm, output_table=alpha_stat, filter_set=(1,2,3,4,5), group_var=intval_group);*/
/*%event_addindex_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, buy_win=0, output_table=alpha_stat, group_var=intval_group);*/
/* 中位数的index */
/*%event_addindex_stat(stat_table=alpha_stat, rtn_var=pct50, buy_win=0, output_table=alpha_stat, group_var=intval_group);*/
/**/
/*%event_mdf_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, output_table=alpha_stat_n,*/
/*					group_var=intval_group, win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120));*/
/*%event_mdf_stat(stat_table=alpha_stat, rtn_var=pct50, output_table=alpha_stat_n,*/
/*					group_var=intval_group, win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120));*/

/** 只计算date_end= win_date的记录(即整个持有周期) */
%LET ret_var = accum_alpha_sm;
DATA cross_rtn_subset;
	SET cross_rtn;
	IF date_end = win_date;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT intval_group,
		count(1) AS nobs,
		mean(win) AS win,
		mean(&ret_var.) AS &ret_var.,
		mean(&ret_var.)/mean(win)*250 AS &ret_var._annual,
		sum(&ret_var.>0)/count(1) AS prob
	FROM cross_rtn_subset
	GROUP BY intval_group;
QUIT;
/**/
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT report_period,*/
/*		count(1) AS nobs,*/
/*		mean(&ret_var.) AS &ret_var.,*/
/*		mean(win) AS win,*/
/*		mean(&ret_var.)/mean(win)*250 AS &ret_var._annual*/
/*	FROM cross_rtn_subset*/
/*	GROUP BY report_period;*/
/*QUIT;*/

/** 分年份 **/
PROC SQL;
	CREATE TABLE stat AS
	SELECT report_period, intval_group,
		count(1) AS nobs,
		mean(win) AS win,
		mean(&ret_var.) AS &ret_var.,
		mean(&ret_var.)/mean(win)*250 AS &ret_var._annual,
		sum(&ret_var.>0)/count(1) AS prob
	FROM cross_rtn_subset
	GROUP BY report_period,intval_group;
QUIT;
PROC TRANSPOSE DATA = stat OUT = stat1;
	BY report_period;
	ID intval_group;
	VAR &ret_var.;
RUN;
PROC TRANSPOSE DATA = stat OUT = stat2;
	BY report_period;
	ID intval_group;
	VAR nobs;
RUN;




