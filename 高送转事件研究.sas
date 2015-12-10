%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 

%LET input_dir = &product_dir.\input_data\高送转; 
%LET output_dir = &product_dir.\output_data\高送转;
LIBNAME product "&product_dir.\sasdata\高送转";

LIBNAME database "D:\Research\数据库\通用";

%INCLUDE "&utils_dir.\日期_通用函数.sas";
%INCLUDE "&utils_dir.\交易_通用函数.sas";
%INCLUDE "&utils_dir.\权重_通用函数.sas";
%INCLUDE "&utils_dir.\组合构建_通用函数.sas";
%INCLUDE "&utils_dir.\其他_通用函数.sas";
%INCLUDE "&utils_dir.\事件研究_通用函数_版本2.sas";
%INCLUDE "&utils_dir.\因子有效性_通用函数.sas";
%INCLUDE "&utils_dir.\计量_通用函数.sas";
%INCLUDE "&utils_dir.\因子计算_通用函数.sas";
%INCLUDE "&utils_dir.\绩效评估_通用函数.sas";

options validvarname=any; /* 支持中文变量名 */

/*** step0: 调用事件研究_配置文件 **/
%LET env_start_date = 30jun2005;
%LET index_code = 000905;
%INCLUDE "&utils_dir.\事件研究_配置文件_本地版本_适合版本2.sas";

/******** 高送转事件基本属性 ********/
/** 1-分红表：
	(1) stock_id
	(2) process: 实施
	(3) ggrq: 公告日期(numeric)
	(4) fh_year：分红年份
	(5) iffz: 
	(6) sg_fm/sg_fz: 送股分母/分子
	(7) zz_fm/zz_fz: 转增分母/分子
	(8) px_fm/px_fzsq/px_fzsh：派现分母/派现分子税前/派现分子税后
	(9) gqdjr/cqcxr/yaggr/gddhggr/fhssggr: 股权登记日/除权除息日/预案公告日/股东大会公告日/分红实施公告日
	(10) year：预案公告日所在的年份
***/

/**** Step1: 基本统计分析 */
/** Step1-1: 送转比例(送股+转股) */
DATA fh;
	SET product.fh;
	sz_fz = coalesce(sg_fz,0)+coalesce(zz_fz,0);
RUN;

PROC SQL;
	CREATE TABLE fh_sz_class AS
	SELECT year, sum(sz_fz>0) AS  nobs_sz,
		sum(floor(sz_fz)>=5) AS nobs_gsz1, /* 高送转1，定义为送转股比例>=5 */
		sum(floor(sz_fz)>=10) AS nobs_gsz2, /* 高送转2： 送转股比例>=10 */
		sum(floor(sz_fz)=1) AS nobs_sz1,
		sum(floor(sz_fz)=2) AS nobs_sz2,
		sum(floor(sz_fz)=3) AS nobs_sz3,
		sum(floor(sz_fz)=4) AS nobs_sz4,
		sum(floor(sz_fz)=5) AS nobs_sz5,
		sum(floor(sz_fz)=6) AS nobs_sz6,
		sum(floor(sz_fz)=7) AS nobs_sz7,
		sum(floor(sz_fz)=8) AS nobs_sz8,
		sum(floor(sz_fz)=9) AS nobs_sz9,
		sum(floor(sz_fz)=10) AS nobs_sz10,
		sum(floor(sz_fz)>10) AS nobs_sz_others
	FROM fh
	GROUP BY year;
QUIT;


/** Step1-2: 分红收益率(税后收益率/预案公告日的前收盘价格) */
DATA fh;
	SET fh;
	yaggr_date = input(put(yaggr,8.),yymmdd10.);
	FORMAT yaggr_date yymmdd10.;
RUN;

%adjust_date_modify(busday_table=busday, raw_table=fh, colname=yaggr_date,  output_table=fh, is_forward =0);
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.year, A.stock_id, A.adj_yaggr_date AS yaggr, A.sz_fz,
		B.pre_close,
		coalesce(A.px_fzsh/B.pre_close/10,0) AS px_rate,
		A.fh_year
	FROM fh A LEFT JOIN hqinfo B
	ON A.stock_id = B.stock_code AND A.adj_yaggr_date = B.end_date
	ORDER BY year, stock_id, yaggr;
QUIT;

DATA fh;
	SET tmp;
RUN;
/** 以0.005分档 */
PROC SQL;
	CREATE TABLE fh_px_class AS
	SELECT year, sum(px_rate>0) AS  nobs_sz,
		sum(floor(px_rate/0.005)>=2) AS nobs_gpx, /* 高派现，定义为送转股比例>=5 */
		sum(floor(px_rate/0.005)=1) AS nobs_px1,
		sum(floor(px_rate/0.005)=2) AS nobs_px2,
		sum(floor(px_rate/0.005)=3) AS nobs_px3,
		sum(floor(px_rate/0.005)=4) AS nobs_px4,
		sum(floor(px_rate/0.005)=5) AS nobs_px5,
		sum(floor(px_rate/0.005)=6) AS nobs_px6,
		sum(floor(px_rate/0.005)=7) AS nobs_px7,
		sum(floor(px_rate/0.005)=8) AS nobs_px8,
		sum(floor(px_rate/0.005)=9) AS nobs_px9,
		sum(floor(px_rate/0.005)=10) AS nobs_px10,
		sum(floor(px_rate/0.005)>10) AS nobs_px_others
	FROM fh
	GROUP BY year;
QUIT;

/** Step1-3: 分为3组统计：
	(1) 高送转1，高派现
	(2) 高送转1, 低派现
	(3) 低送转，高派现
	(4) 低送转，低派现
***/
PROC SQL;
	CREATE TABLE fh_szpx_class AS
	SELECT year, count(1) AS nobs,
		sum(floor(sz_fz)>=10 AND floor(px_rate/0.005)>=2) AS gszgpx,
		sum(floor(sz_fz)>=10 AND floor(px_rate/0.005)<2) AS gszdpx,
		sum(floor(sz_fz)<10 AND floor(px_rate/0.005)>=2) AS dszgpx,
		sum(floor(sz_fz)<10 AND floor(px_rate/0.005)<2) AS dszdpx
	FROM fh
	GROUP BY year;
QUIT;


/**************************** Step2: 事件研究 */
/** 10送5及以上 */
PROC SQL;
	CREATE TABLE gsz AS
	SELECT distinct stock_id AS stock_code LABEL "stock_code",
		yaggr AS event_date, year
	FROM fh
	WHERE floor(sz_fz)>=10
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
%event_cal_stat(rtn_table=gsz_rtn, rtn_var=accum_alpha_sm, output_table=alpha_stat, filter_set=(1,2,3,4,5), group_var=year);
%event_addindex_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, buy_win=0, output_table=alpha_stat, group_var=year);
/* 中位数的index */
%event_addindex_stat(stat_table=alpha_stat, rtn_var=pct50, buy_win=0, output_table=alpha_stat, group_var=year);

%event_mdf_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, output_table=alpha_stat_n,
					group_var=year, win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120));


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

/********************* Step3: 预测效果 **************/
/** 注意：
(1) 以期末总股本(qmzgb)字段为基准。另外share(原：股本字段)历史上有361条记录不等于期末总股本字段。原因未知.
(2) 只选用acc_type=2(新会计准则) & report_type="合并报表"的记录
(3) 20061231开始采用新会计准则的上市公司数量才稳定

**/
PROC SQL;
	CREATE TABLE stat1 AS
	SELECT report_period, count(1) AS nobs
	FROM product.zcfzb
	WHERE acc_type = "2" AND report_type="合并报表"
	GROUP BY report_period;
QUIT;

/** Step3-1预测指标如下(以三季报的数据为准) */
/* 	(1) 总股本
	(2) 每股净资产
	(3) 每股资本公积
	(4) 每股未分配利润
	(5) 每股留存收益 = 未分配利润 + 盈余公积
	(6) 每股资本公积+留存收益
	(7) 股价(平均一个月估计)
**/

/* 取三季度财报数据，从2007年开始 */
PROC SQL;
	CREATE TABLE fdata AS
	SELECT stock_code, report_period, report_date, 
		qmzgb, 
		(assets-liability)/qmzgb AS net_assets,
		coalesce(zbgj,0)/qmzgb AS zbgj, 
		coalesce(wfplr,0)/qmzgb AS wfplr,
		(coalesce(yygj,0)+coalesce(wfplr,0))/qmzgb AS lcsy,
		(coalesce(zbgj,0)+coalesce(yygj,0)+coalesce(wfplr,0))/qmzgb AS zbgj_lcsy
	FROM product.zcfzb
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
		WHEN coalesce(B.sz_fz,0)>= 5 THEN 1
		ELSE 0
	END AS is_gsz,
	floor(report_period/10000) AS year
	FROM fdata A LEFT JOIN 
	(SELECT * FROM fh WHERE fh_year-floor(fh_year/10000)*10000=1231 AND not missing(sz_fz)) B  /* 只保留年报 */
	ON A.stock_code = B.stock_id AND floor(B.fh_year/10000) = floor(A.report_period/10000)
	WHERE floor(report_period/10000) >= 2007 AND floor(report_period/10000) <= 2014
	ORDER BY A.report_period, A.stock_code;
QUIT;

/** 计算每组高送转概率(precision)，每组平均送转比例和查全率(recall) */
/** 新增指标：F-score = 2*precision*recall/(precision+recall) */

%LET factor = net_assets;
%LET desc_str = descending;

PROC RANK DATA = fdata_fh OUT = fdata_fh_rank TIES = low &desc_str. GROUPS = 10;
	VAR &factor.;
	RANKS r_&factor.;
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
			sum(is_gsz=1) AS n_gsz,
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


		


	
