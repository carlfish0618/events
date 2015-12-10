/* 事件研究 */
%LET event_start_date = 31dec2008;
%LET event_end_date = 31oct2015;


%adjust_date_to_mapdate(rawdate_table=gqjl, mapdate_table=busday, raw_colname=yaggr, 
		map_colname=date, output_table=gqjl_s,is_backward=0, is_included=1);
%adjust_date_to_mapdate(rawdate_table=gqjl_s, mapdate_table=busday, raw_colname=gddhr, 
		map_colname=date, output_table=gqjl_s,is_backward=0, is_included=1);
%adjust_date_to_mapdate(rawdate_table=gqjl_s, mapdate_table=busday, raw_colname=ssggr, 
		map_colname=date, output_table=gqjl_s,is_backward=0, is_included=1);


%LET in_date = map_yaggr;
%LET out_date = map_gddhr;
/** 设定进入日和退出日 */
PROC SQL;
	CREATE TABLE cross_sample AS
	SELECT stock_code AS stock_code LABEL "stock_code",
		&in_date. AS event_date,
		&out_date. AS date_end
	FROM gqjl_s
	WHERE  "&event_start_date."d <= &in_date. <= "&event_end_date."d
	AND not missing(&in_date.) AND not missing(&out_date.)
	ORDER BY stock_code, &in_date.;
QUIT;
PROC SORT DATA = cross_sample NODUPKEY OUT = cross_sample;
	BY stock_code event_date;
RUN;

DATA cross_sample;
	SET cross_sample;
	event_id = _N_;
RUN;


/************************************** 以下适合所有的事件研究 ***********************************/
%event_gen_windows(event_table=cross_sample, start_win=0, end_win=750, busday_table = busday, output_table=cross_win);
/*%event_gen_fixed_windows(event_table=cross_sample, start_col=event_date, end_col=date_end, busday_table = busday, output_table=cross_win);*/

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
%event_smooth_rtn(rtn_table=cross_rtn, buy_win=0, suffix=sm, output_table=cross_rtn, filter_set=(10,20,21,22,30,40,41,50));
/** 剔除连续涨跌停超过5个交易日 **/
%event_second_smooth_rtn(rtn_table=cross_rtn, buy_win=0, suffix=sm, output_table=cross_rtn, threshold = 20, filter_set=(60,61,40,41));
/** 剔除连续停牌超过20个交易日 */
%event_second_smooth_rtn(rtn_table=cross_rtn, buy_win=0, suffix=sm, output_table=cross_rtn, threshold = 365, filter_set=(10));
/** 剔除超出上市区间 */
%event_second_smooth_rtn(rtn_table=cross_rtn, buy_win=0, suffix=sm, output_table=cross_rtn, threshold = 1, filter_set=(30));


/** 统计指标 */
%event_cal_stat(rtn_table=cross_rtn, rtn_var=accum_alpha_sm, output_table=alpha_stat, filter_set=(.), group_var=);
%event_addindex_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, buy_win=0, output_table=alpha_stat, group_var=);
/* 中位数的index */
%event_addindex_stat(stat_table=alpha_stat, rtn_var=pct50, buy_win=0, output_table=alpha_stat, group_var=);

%event_mdf_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, output_table=alpha_stat_n,
					group_var=, win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120,250,500,750));
%event_mdf_stat(stat_table=alpha_stat, rtn_var=pct50, output_table=alpha_stat_n,
					group_var=, win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120,250,500,750));



/** 只计算date_end= win_date的记录(即整个持有周期) */
%LET ret_var = accum_alpha_sm;
DATA cross_rtn_subset;
	SET cross_rtn;
	IF date_end = win_date;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT 
		count(1) AS nobs,
		mean(win) AS win,
		mean(&ret_var.) AS &ret_var.,
		mean(&ret_var.)/mean(win)*250 AS &ret_var._annual,
		sum(&ret_var.>0)/count(1) AS prob
	FROM cross_rtn_subset;
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

