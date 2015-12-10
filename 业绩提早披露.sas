

/*************** Step4A: 事件研究：截面分组，考察是否在真实财报发布前后会有差异 */
%LET event_start_date = 31dec2010;
%LET event_end_date = 31oct2015;

/* 将actdate调整到交易日 */
%adjust_date_to_mapdate(rawdate_table=yypl, mapdate_table=busday, raw_colname=actdate, 
		map_colname=date, output_table=yypl,is_backward=0, is_included=1);
PROC SORT DATA = yypl;
	BY report_period intval;
RUN;
PROC RANK DATA = yypl OUT = yypl_rank TIES = low GROUPS = 20;
	WHERE not missing(intval);
	BY report_period;
	VAR intval;
	RANKS r_intval;
RUN;

PROC SQL;
	CREATE TABLE cross_sample AS
	SELECT distinct stock_code AS stock_code LABEL "stock_code",
		report_period,
		map_actdate AS event_date,
		r_intval,
		intval
	FROM yypl_rank
	WHERE not missing(map_actdate) AND "&event_start_date."d <= map_actdate <= "&event_end_date."d
	AND period = 12  /* 年报 */
	AND date1_index-pub_date_index+1>=20
	ORDER BY map_actdate, stock_code;
QUIT;
DATA cross_sample;
	SET cross_sample;
	event_id = _N_;
RUN;

%event_gen_windows(event_table=cross_sample, start_win=-40, end_win=20, busday_table = busday, output_table=cross_win);
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
%event_cal_stat(rtn_table=cross_rtn, rtn_var=accum_alpha_sm, output_table=alpha_stat, filter_set=(1,2,3,4,5), group_var=r_intval);
%event_addindex_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, buy_win=0, output_table=alpha_stat, group_var=r_intval);
/* 中位数的index */
%event_addindex_stat(stat_table=alpha_stat, rtn_var=pct50, buy_win=0, output_table=alpha_stat, group_var=r_intval);

%event_mdf_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, output_table=alpha_stat_n,
					group_var=r_intval, win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120));


/*** 结果在探查 */
PROC SQL;
	CREATE TABLE cross_sample AS
	SELECT *,
	date1_index-pub_date_index+1 AS intval_days
	FROM yypl_rank
	WHERE not missing(map_actdate) AND "&event_start_date."d <= map_actdate <= "&event_end_date."d
	AND period = 12  /* 年报 */
	AND r_intval = 0
	ORDER BY map_actdate, stock_code;
QUIT;
%cal_dist(input_table=cross_sample, by_var=period, cal_var=intval_days, out_table=stat);


/*************** Step4B: 事件研究：不同组从预案披露日期到发布日之间的收益 */
%LET event_start_date = 31dec2010;
%LET event_end_date = 31oct2015;

/* 将actdate调整到交易日 */
%adjust_date_to_mapdate(rawdate_table=yypl, mapdate_table=busday, raw_colname=actdate, 
		map_colname=date, output_table=yypl,is_backward=0, is_included=1);
PROC SORT DATA = yypl;
	BY report_period intval;
RUN;
PROC RANK DATA = yypl OUT = yypl_rank TIES = low GROUPS = 20;
	WHERE not missing(intval);
	BY report_period;
	VAR intval;
	RANKS r_intval;
RUN;

PROC SQL;
	CREATE TABLE cross_sample AS
	SELECT distinct stock_code AS stock_code LABEL "stock_code",
		report_period,
		map_actdate AS event_date,
		r_intval,
		intval,
		pub_date AS date_start,
		deadline AS date_end
	FROM yypl_rank
	WHERE not missing(map_actdate) AND "&event_start_date."d <= map_actdate <= "&event_end_date."d
	AND period = 12  /* 年报 */
	ORDER BY map_actdate, stock_code;
QUIT;
DATA cross_sample;
	SET cross_sample;
	event_id = _N_;
RUN;

%event_gen_fixed_windows(event_table=cross_sample, start_col=date_start, end_col=date_end, busday_table = busday, output_table=cross_win);
/** 统计时间跨度分布 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT min_win, count(1) AS nobs
	FROM(
		SELECT event_id, 
			max(win) AS max_win,
			min(win) AS min_win
		FROM cross_win
		GROUP BY event_id
	)
	GROUP BY min_win;
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
%event_cal_stat(rtn_table=cross_rtn, rtn_var=accum_alpha_sm, output_table=alpha_stat, filter_set=(1,2,3,4,5), group_var=r_intval);
%event_addindex_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, buy_win=0, output_table=alpha_stat, group_var=r_intval);
/* 中位数的index */
%event_addindex_stat(stat_table=alpha_stat, rtn_var=pct50, buy_win=0, output_table=alpha_stat, group_var=r_intval);

%event_mdf_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, output_table=alpha_stat_n,
					group_var=r_intval, win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120));



