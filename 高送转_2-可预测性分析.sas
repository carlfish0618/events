/**高送转是否可预测，只针对年报数据　**/


/********************** Step1: 预测效果 **************/
/** 注意：
(1) 以期末总股本(qmzgb)字段为基准。另外share(原：股本字段)历史上有361条记录不等于期末总股本字段。原因未知.
(2) 只选用acc_type=2(新会计准则) & report_type="合并报表"的记录
(3) 20061231开始采用新会计准则的上市公司数量才稳定

**/
PROC SQL;
	CREATE TABLE stat1 AS
	SELECT report_period, count(1) AS nobs
	FROM event_db.zcfzb
	WHERE acc_type = "2" AND report_type="合并报表"
	GROUP BY report_period;
QUIT;

/** Step1: 构造预测指标 */
/* 	(1) 总股本
	(2) 每股净资产
	(3) 每股资本公积
	(4) 每股未分配利润
	(5) 每股留存收益 = 未分配利润 + 盈余公积
	(6) 每股资本公积+留存收益
	(7) 股价(平均一个月估计)
	(8) 上市时间
**/

/* (1) 取三季度财报数据，从2007年开始 */
PROC SQL;
	CREATE TABLE fdata AS
	SELECT stock_code, report_period, report_date, 
		qmzgb, 
		(assets-liability)/qmzgb AS net_assets,
		coalesce(zbgj,0)/qmzgb AS zbgj, 
		coalesce(wfplr,0)/qmzgb AS wfplr,
		(coalesce(yygj,0)+coalesce(wfplr,0))/qmzgb AS lcsy,
		(coalesce(zbgj,0)+coalesce(yygj,0)+coalesce(wfplr,0))/qmzgb AS zbgj_lcsy
	FROM event_db.zcfzb
	WHERE acc_type = "2" AND report_type="合并报表" AND report_period-floor(report_period/10000)*10000=930
		AND not missing(qmzgb)
	ORDER BY report_period, stock_code;
QUIT;

/** 与送转信息连接 */
PROC SQL;
	CREATE TABLE fdata_fh AS
	SELECT A.*, coalesce(B.sz_fz,0) AS sz_fz,
		B.yaggr,
	CASE 
		WHEN coalesce(B.sz_fz,0)>= 10 THEN 1
		ELSE 0
	END AS is_gsz,
	CASE 
		WHEN B.sz_fz > 0 THEN 1
		ELSE 0
	END AS is_sz,
	floor(report_period/10000) AS year
	FROM fdata A LEFT JOIN 
	(SELECT * FROM fh WHERE fh_year-floor(fh_year/10000)*10000=1231 AND not missing(sz_fz)) B  /* 只保留年报 */
	ON A.stock_code = B.stock_code AND floor(B.fh_year/10000) = floor(A.report_period/10000)
	WHERE floor(report_period/10000) >= 2007 AND floor(report_period/10000) <= 2014
	ORDER BY A.report_period, A.stock_code;
QUIT;

/*** (2) 与行情信息相连接，获得10月平均股价 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.stock_code, A.report_period,
			B.date AS end_date,
			C.close
	FROM fdata_fh A LEFT JOIN busday B
	ON A.year = year(B.date) AND month(B.date) IN (10)
	LEFT JOIN hqinfo C
	ON A.stock_code = C.stock_code AND B.date = C.end_date
	ORDER BY A.report_period, A.stock_code, end_date;
QUIT;
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, B.close AS avg_close
	FROM fdata_fh A LEFT JOIN(
		SELECT report_period, stock_code, mean(close) AS close
		FROM tmp
		GROUP BY report_period, stock_code)B 
	ON A.report_period = B.report_period AND A.stock_code = B.stock_code
	ORDER BY A.report_period, A.stock_code;
QUIT;
DATA fdata_fh;
	SET tmp2;
RUN;

/*** (3) 上市时间 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.list_date 
	FROM fdata_fh A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code
	ORDER BY A.report_period, A.stock_code;
QUIT;
DATA fdata_fh;
	SET tmp;
RUN;


/** 计算每组高送转概率(precision)，每组平均送转比例和查全率(recall) */
/** 新增指标：F-score = 2*precision*recall/(precision+recall) */

%LET factor = list_date;
%LET desc_str = descending;

PROC RANK DATA = fdata_fh OUT = fdata_fh_rank TIES = low &desc_str. GROUPS = 10;
	VAR &factor.;
	RANKS r_&factor.;
RUN;

/** 离散变量不用分组的形式，如上市年份 */

DATA fdata_fh_rank;
	SET fdata_fh;
	IF not missing(list_date) THEN DO;
		IF 0 < input(put(report_period,8.),yymmdd10.)+31 - list_date <= 365 THEN r_list_date = 1;
		ELSE IF input(put(report_period,8.),yymmdd10.)+31 - list_date <= 365 * 3 THEN r_list_date = 2;
		ELSE IF input(put(report_period,8.),yymmdd10.)+31 - list_date <= 365 * 5 THEN r_list_date = 3;
		ELSE IF input(put(report_period,8.),yymmdd10.)+31 - list_date <= 365 * 10 THEN r_list_date = 4;
		ELSE IF input(put(report_period,8.),yymmdd10.)+31 - list_date > 365 * 10 THEN r_list_date = 5;
		ELSE r_list_date = .;
	END;
	ELSE r_list_date = .;
RUN;



PROC SQL;
	CREATE TABLE fdata_fh_class AS
	SELECT A.*, 
		A.n_gsz/B.nobs AS recall,
		2*(A.n_gsz/B.nobs)*precision/(A.n_gsz/B.nobs+A.precision) AS f_score,
		B.nobs AS total_gsz
	FROM(
		SELECT r_&factor., 
			count(1) AS nobs,
			sum(is_sz=1) AS n_sz,
			sum(is_gsz=1) AS n_gsz,
			sum(is_sz=1)/count(1) AS prob,
			mean(sz_fz/10) AS sz,
			sum(is_gsz=1)/count(1) AS precision
		FROM fdata_fh_rank
		GROUP BY r_&factor.) A,
		(SELECT sum(is_gsz=1) AS nobs
		FROM fdata_fh_rank) B;
QUIT;


/** 分年份 */
PROC RANK DATA = fdata_fh OUT = fdata_fh_rank TIES = low &desc_str. GROUPS = 10;
	BY year;
	VAR &factor.;
	RANKS r_&factor.;
RUN;
PROC SQL;
	CREATE TABLE fdata_fh_class_year AS
	SELECT A.*, 
		A.n_gsz/B.nobs AS recall,
		2*(A.n_gsz/B.nobs)*precision/(A.n_gsz/B.nobs+A.precision) AS f_score,
		B.nobs AS total_gsz
	FROM(
		SELECT year, r_&factor., 
			count(1) AS nobs,
			sum(is_gsz=1) AS n_gsz,
			mean(sz_fz/10) AS sz,
			sum(is_gsz=1)/count(1) AS precision
		FROM fdata_fh_rank
		GROUP BY year, r_&factor.) A LEFT JOIN (
		SELECT year, 
			sum(is_gsz=1) AS nobs
		FROM fdata_fh_rank
		GROUP BY year) B
		ON A.year= B.year;
QUIT;
/** 转置可选 */
PROC TRANSPOSE DATA = fdata_fh_class_year OUT = fdata_fh_class_year;
	BY year;
	ID r_&factor.;
RUN;
PROC SORT DATA = fdata_fh_class_year(DROP=_LABEL_ RENAME=(_NAME_=variable));
	BY variable year;
RUN;


/** Step3-2 验证：高预测组是否等价于高的超额收益? */
/** 方法：分为两大类
	(1)真实发生高送转，内部分为0-4组，按照前期预测得分从高到低分组
	(2)未发生高送转组，内部分为0-4组，按照前期预测得分从高到低分组(这类因为无预案公告日，因此统一设定为11月1日)
**/
%LET factor = zbgj;
%LET desc_str = descending;

PROC RANK DATA = fdata_fh OUT = fdata_fh_rank TIES = low &desc_str. GROUPS = 10;
	WHERE is_gsz = 1;
	VAR &factor.;
	RANKS r_&factor.;
RUN;

PROC SQL;
	CREATE TABLE gsz AS
	SELECT distinct stock_code AS stock_code LABEL "stock_code",
		yaggr AS event_date, r_&factor.
	FROM fdata_fh_rank
	ORDER BY yaggr, stock_code;
QUIT;
DATA gsz;
	SET gsz;
	event_id = _N_;
RUN;

%event_gen_windows(event_table=gsz, start_win=-120, end_win=120, busday_table = busday, output_table=gsz_win);
%event_get_marketdata(win_table=gsz_win, buy_win=0, hq_table=hqinfo, bm_table=bm_hqinfo, busday_table=busday, output_table=gsz_rtn);
%event_mark_win(rtn_table=gsz_rtn, stock_info_table=stock_info_table, output_table=gsz_rtn);
/** 分析filter分布 */
PROC SQL;
	CREATE TABLE fh_filter_stat AS
	SELECT win, filter, count(1) AS nobs
	FROM gsz_rtn
	GROUP BY win, filter;
QUIT;
PROC TRANSPOSE DATA = fh_filter_stat OUT = fh_filter_stat;
	BY win;
	ID filter;
RUN;

%event_cal_accum_alpha(rtn_table=gsz_rtn,buy_win=0, output_table=gsz_rtn);
%event_smooth_rtn(rtn_table=gsz_rtn, buy_win=0, suffix=sm, output_table=gsz_rtn, filter_set=(1,2,3,4,5));

/** 统计指标 */
%event_cal_stat(rtn_table=gsz_rtn, rtn_var=accum_alpha_sm, output_table=alpha_stat, filter_set=(1,2,3,4,5), group_var=r_&factor.);
%event_addindex_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, buy_win=0, output_table=alpha_stat, group_var=r_&factor.);
/* 中位数的index */
%event_addindex_stat(stat_table=alpha_stat, rtn_var=pct50, buy_win=0, output_table=alpha_stat, group_var=r_&factor.);

%event_mdf_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, output_table=alpha_stat_n,
					group_var=r_&factor., win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120));


/** 年份只保留均值结果 */
DATA stat;
	SET alpha_stat;
	KEEP year win accum_alpha_sm_index;
RUN;
PROC SORT DATA = stat;
	BY win;
RUN;
PROC TRANSPOSE DATA=stat OUT = stat;
	BY win;
	ID year;
	VAR accum_alpha_sm_index;
RUN;


