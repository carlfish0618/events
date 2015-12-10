/** 业绩提早披露：统计分析 */
/*** 策略开始时间为：2006/1/1 ****/
/** 外部数据库：
(1) event_db.yypl: 预约披露表
(2) event_db.earning_actual: 净利润表
***/


%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 

%LET input_dir = &product_dir.\input_data\业绩提早披露; 
%LET output_dir = &product_dir.\output_data\业绩提早披露;
LIBNAME product "&product_dir.\sasdata\业绩提早披露";

LIBNAME database "D:\Research\数据库\通用";
LIBNAME event_db "D:\Research\数据库\事件数据库";

%INCLUDE "&utils_dir.\日期_通用函数.sas";
%INCLUDE "&utils_dir.\其他_通用函数.sas";
%INCLUDE "&utils_dir.\事件研究_通用函数_版本2.sas";


%INCLUDE "&utils_dir.\交易_通用函数.sas";
%INCLUDE "&utils_dir.\权重_通用函数.sas";
%INCLUDE "&utils_dir.\组合构建_通用函数.sas";
%INCLUDE "&utils_dir.\因子有效性_通用函数.sas";
%INCLUDE "&utils_dir.\计量_通用函数.sas";
%INCLUDE "&utils_dir.\因子计算_通用函数.sas";
%INCLUDE "&utils_dir.\绩效评估_通用函数.sas";

options validvarname=any; /* 支持中文变量名 */


/******************* Step0: 调用事件研究_配置文件 **/
%LET env_start_date = 30jun2005;
%LET index_code = 000905;
%INCLUDE "&utils_dir.\事件研究_配置文件_本地版本_适合版本2.sas";

/******************* Step1: 补齐信息：正式财报预约披露时间 */
/** 1季报：发布时间为4月，预约披露时间为4月第一个交易日开盘前 */
/** 2季报: 发布时间为7-8月，预约披露时间为7月第一个交易日开盘前 */
/** 3季报：发布时间为10月，预约披露时间为10月第一个交易日开盘前 */
/** 年报：发布时间为来年1-4月，预约披露时间为来年第一个交易日开盘前 */

/** 月末日期 */
%get_month_date(busday_table=busday, start_date=01jan200, end_date=31dec2100, rename=end_date, output_table=month_end, type=1);
PROC SQL;
	CREATE TABLE yypl AS
	SELECT stock_code,
		datepart(perioddate) FORMAT yymmdd10. AS report_period,
		month(datepart(perioddate)) AS period,
		datepart(date1) FORMAT yymmdd10. AS date1,
		datepart(date2) FORMAT yymmdd10. AS date2,
		datepart(date3) FORMAT yymmdd10. AS date3,
		datepart(date4) FORMAT yymmdd10. AS date4,
		datepart(actdate) FORMAT yymmdd10. AS actdate,
		B.end_date AS period_end_date LABEL "period_end_date"
	FROM event_db.yypl A LEFT JOIN month_end B
	ON month(datepart(A.perioddate)) = month(B.end_date) AND year(datepart(A.perioddate)) = year(B.end_date)
	WHERE not missing(B.end_date)
	ORDER BY report_period, stock_code;
QUIT;
/** 下一个交易日 */
%adjust_date_to_mapdate(rawdate_table=yypl, mapdate_table=busday, raw_colname=period_end_date, 
		map_colname=date, output_table=yypl,is_backward=0, is_included=0);
DATA yypl;
	SET yypl;
	RENAME map_period_end_date = pub_date;
	LABEL map_period_end_date = "pub_date";
	change_time = not missing(date2) + not missing(date3) + not missing(date4);
RUN;

/** 统计正式财报发布日期的区间*/
/** 预约日期都符合要求，但是正式发布日期有些不符合要求 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT report_period, 
		max(actdate) AS max FORMAT yymmdd10.,
		min(actdate) AS min FORMAT yymmdd10.,
		max(date1) AS max_date1 FORMAT yymmdd10.,
		min(date1) AS min_date1 FORMAT yymmdd10.
	FROM yypl
	GROUP BY report_period;
/*	HAVING max ~= max_date1;*/
QUIT;


/**【统计分析1- 预约披露修改次数】**/
PROC SQL;
	CREATE TABLE stat AS
	SELECT report_period, 
		count(1) AS nobs,
		sum(change_time=0) AS nobs0,
		sum(change_time=1) AS nobs1,
		sum(change_time=2) AS nobs2,
		sum(change_time=3) AS nobs3
	FROM yypl
	GROUP BY report_period;
QUIT;

/**【统计分析2- 预约披露最后日期】**/
DATA tt;
	SET yypl;
	final_pl = sum(date1*(change_time=0),date2*(change_time=1), date3*(change_time=2),date4*(change_time=3));
	FORMAT final_pl yymmdd10.;
	dif = actdate-final_pl;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT report_period,
		count(1) AS nobs,
		sum(dif=0) AS nobs0,
		sum(dif=-1) AS nobs1,
		sum(dif=1) AS nobs2,
		sum(dif<-1) AS nobs3,
		sum(dif>1) AS nobs4
	FROM tt
	GROUP BY report_period;
QUIT;

/**【统计分析3-第一次预约日期和实际发布日期的时间分布,只统计年报】**/
DATA tt;
	SET yypl;
	date1_m = month(date1);
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT report_period,
		date1_m,
		count(1) AS nobs
	FROM tt
	WHERE period = 12
	GROUP BY report_period, date1_m;
QUIT;
PROC TRANSPOSE DATA = stat OUT = stat prefix = M;
	BY report_period;
	ID date1_m;
	VAR nobs;
RUN;

/************************** Step2: 间隔时间的计算********/

/** Step2-1: 统计限定的时间之间的交易日个数，并计算真实发布和预约时间在该区间的分位点*/
PROC SQL;
	CREATE TABLE yypl2 AS
	SELECT *,
		CASE 
			WHEN month(pub_date) IN (4,10) THEN mdy(month(pub_date),1,year(pub_date))  /* 一季报和三季报，只有一个月时间 */
			WHEN month(pub_date) = 7 THEN mdy(month(pub_date)+1,1,year(pub_date)) /* 中报：2个月时间 */
			WHEN month(pub_date) = 1 THEN mdy(4,1,year(pub_date))
		END AS deadline FORMAT yymmdd10.
	FROM yypl
	ORDER BY report_period;
QUIT;
/* 拿月初日期获取月底日期 */
%adjust_date_to_mapdate(rawdate_table=yypl2, mapdate_table=month_end, raw_colname=deadline, 
		map_colname=end_date, output_table=yypl2,is_backward=0, is_included=1);
DATA yypl;
	SET yypl2(drop = deadline);
	RENAME map_deadline = deadline;
	LABEL map_deadline = "deadline";
RUN;

/***统计交易日个数 */
/* 将date1调整到交易日 */
%adjust_date_to_mapdate(rawdate_table=yypl, mapdate_table=busday, raw_colname=date1, 
		map_colname=date, output_table=yypl,is_backward=0, is_included=1);
%map_date_to_index(busday_table=busday, raw_table=yypl, date_col_name=map_date1, raw_table_edit=yypl, index_name=date1_index);
/* 发布起始和结束日期 */
%map_date_to_index(busday_table=busday, raw_table=yypl, date_col_name=pub_date, raw_table_edit=yypl, index_name=pub_date_index);
%map_date_to_index(busday_table=busday, raw_table=yypl, date_col_name=deadline, raw_table_edit=yypl, index_name=deadline_index);

/**相对区间*/
/** 区分交易日和自然日 */
DATA yypl;
	SET yypl;
	period = month(report_period);
	IF period = 3 THEN DO;
		pub_date0 = mdy(4,1,year(report_period));
		deadline0 = mdy(4,30,year(report_period));
	END;
	ELSE IF period = 6 THEN DO;
		pub_date0 = mdy(7,1,year(report_period));
		deadline0 = mdy(8,31,year(report_period));
	END;
	ELSE IF period = 9 THEN DO;
		pub_date0 = mdy(10,1,year(report_period));
		deadline0 = mdy(10,31,year(report_period));
	END;
	ELSE IF period = 12 THEN DO;
		pub_date0 = mdy(1,1,year(report_period)+1);
		deadline0 = mdy(4,30,year(report_period)+1);
	END;			
	FORMAT pub_date0 deadline0 yymmdd10.;	
	intval_days = date1_index-pub_date_index+1;  /** 交易日 */
	intval = (date1_index-pub_date_index+1)/(deadline_index-pub_date_index+1);
	intval0_days = date1-pub_date0+1;
	intval0 = (date1-pub_date0+1)/(deadline0-pub_date0+1);
RUN;
PROC SQL;
	DROP TABLE yypl2;
QUIT;

/**【统计分析4- 第一次预约披露日期相对年初的时间间隔(交易日)】**/
PROC SGPLOT DATA = yypl;
	WHERE period = 12;
	HISTOGRAM intval_days;
	TITLE "第一次预约披露日期相对年初的时间间隔(交易日)";
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT intval_days, count(1) AS nobs
	FROM yypl
	WHERE period = 12
	GROUP BY intval_days;
QUIT;
DATA yypl_s;
	SET yypl;
	IF period = 12;
	year = year(report_period);
RUN;

%cal_dist(input_table=yypl_s, by_var=period, cal_var=intval_days, out_table=stat, pctlpts=100 99 95 90 75 50 25 10 5 1 0);

/************************************** Step4: 不同退出时点下的持有期 **/

/* 将actdate调整到交易日 */
%adjust_date_to_mapdate(rawdate_table=yypl, mapdate_table=busday, raw_colname=actdate, 
		map_colname=date, output_table=yypl,is_backward=0, is_included=1);
%map_date_to_index(busday_table=busday, raw_table=yypl, date_col_name=map_actdate, raw_table_edit=yypl, index_name=actdate_index);

DATA yypl;
	SET yypl;
	exist_days = actdate_index - pub_date_index;
RUN;

/**【统计分析4B- 正式财报相较年初的时间间隔(交易日)】**/
PROC SGPLOT DATA = yypl;
	WHERE period = 12;
	HISTOGRAM exist_days;
	TITLE "正式财报相较年初的时间间隔(交易日)";
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT exist_days, count(1) AS nobs
	FROM yypl
	WHERE period = 12
	GROUP BY exist_days;
QUIT;
DATA yypl_s;
	SET yypl;
	IF period = 12;
	year = year(report_period);
RUN;

%cal_dist(input_table=yypl_s, by_var=year, cal_var=exist_days, out_table=stat, pctlpts=100 99 95 90 75 50 25 10 5 1 0);
%cal_dist(input_table=yypl_s, by_var=period, cal_var=exist_days, out_table=stat, pctlpts=100 99 95 90 75 50 25 10 5 1 0);


/************************************** Step4: 不同间隔时间与净利润或业绩预告之间的关系 **/
/** Step4-1:计算净利润同比增速 */
PROC SQL;
	CREATE TABLE earning_actual AS
	SELECT stock_code, 
		input(put(report_period, 8.), yymmdd10.) AS report_period FORMAT yymmdd10.,
		report_date,
		earning
	FROM event_db.earning_actual
	ORDER BY report_period, report_date;
QUIT;

PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, 
		B.earning AS pre_earning,
		(A.earning-B.earning)/B.earning AS earning_yoy
	FROM earning_actual A LEFT JOIN earning_actual B
	ON A.stock_code = B.stock_code AND year(A.report_period) = year(B.report_period)+1
	ORDER BY A.report_period, A.report_date;
QUIT;
DATA earning_actual;
	SET tmp;
RUN;

/** Step4-2: 与净利润同比增速相匹配 */
PROC SQL;
	CREATE TABLE yypl_with_earning AS
	SELECT A.*, B.earning_yoy
	FROM yypl A LEFT JOIN earning_actual B
	ON A.stock_code = B.stock_code AND A.report_period = B.report_period
	WHERE A.period = 12
	ORDER BY A.report_period, A.stock_code;
QUIT;

/*** Step4-3: 统计不同间隔区间的净利润增速差异 */
/**【统计分析5- 不同间隔区间的净利润增速差异】**/

DATA yypl_with_earning;
	SET yypl_with_earning;
	intval_group = floor(intval_days/10);
RUN;
%cal_dist(input_table=yypl_with_earning, by_var=intval_group, cal_var=earning_yoy, out_table=stat, 
		pctlpts=100 99 95 90 75 50 25 10 5 1 0);

/** 剔除异常值，正负5% **/
PROC SQL;
	CREATE TABLE yypl_s AS
	SELECT A.*
	FROM yypl_with_earning A LEFT JOIN stat B 
	ON B.intval_group = A.intval_group 	
	WHERE A.earning_yoy>B.p5 AND A.earning_yoy < B.p95
	ORDER BY A.intval_group;
QUIT;

/** 画箱体图 */
/*goptions reset = all;*/
/*symbol value = dot */
/*	height = 0.4 */
/*	interpol = boxtf */
/*	width = 3*/
/*	bwidth = 5 */
/*	co = GREEN*/
/*	cv = YELLOW;*/
/**/
/*axis1 order = (-5 to 5 by 1) */
/*	label = (height = 1.25 angle = 90 'earning_yoy')*/
/*	minor = (number = 1);*/
/*axis2 label = (height = 1.25 'intval_group')*/
/*	offset = (5, 5);*/
/**/
/*proc gplot data = yypl_s;*/
/*	plot earning_yoy* intval_group / haxis = axis2 vaxis = axis1 skipmiss ;*/
/*RUN;*/
/*QUIT;*/

/** Step3-3:与业绩预告相匹配 */
/** 注意1对多的情况 */
DATA yypl_with_earning;
	SET yypl_with_earning;
	event_id = _N_;
RUN;

PROC SQL;
	CREATE TABLE yypl_with_earning2 AS
	SELECT A.*, 
		B.eup_type, 
		B.elow_type, 
		B.eup_ratio, 
		B.elow_ratio,
		B.source,
		datepart(B.reportdate) AS f_date FORMAT yymmdd10.,
		coalesce(B.eup_type, B.elow_type) AS e_type,
	CASE 
		WHEN missing(B.stock_code) THEN 0
		ELSE 1
	END AS have_forecast
	FROM yypl_with_earning A LEFT JOIN event_db.earning_forecast_raw B
	ON A.stock_code = B.stock_code AND A.report_period = input(B.report_period,yymmdd10.)
	ORDER BY A.report_period, A.stock_code;
QUIT;
/** 调整到交易日 */
%adjust_date_to_mapdate(rawdate_table=yypl_with_earning2, mapdate_table=busday, raw_colname=f_date, 
		map_colname=date, output_table=yypl_with_earning2,is_backward=0, is_included=1);

/**【统计分析6- 业绩预告的覆盖程度及最早的业绩预告出现的时间】**/
PROC SORT DATA = yypl_with_earning2 OUT = yypl_s;
	BY event_id f_date descending source;  
RUN;

PROC SORT DATA = yypl_s NODUPKEY OUT = yypl_s;
	BY event_id;
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT report_period, 
		count(distinct event_id) AS nobs,
		sum(have_forecast=0) AS nmiss,
		sum(have_forecast=1 AND map_f_date<= pub_date) AS npub,
		sum(have_forecast=1 AND map_f_date>pub_date) AS n_notpub
	FROM yypl_s
	GROUP BY report_period;
QUIT;

/**【统计分析7- 分组统计：最早的业绩预告出现的时间】**/ 
PROC SQL;
	CREATE TABLE stat AS
	SELECT intval_group,
		count(distinct event_id) AS nobs,
		sum(have_forecast=0) AS nmiss,
		sum(have_forecast=1 AND map_f_date<= pub_date) AS npub,
		sum(have_forecast=1 AND map_f_date>pub_date) AS n_notpub
	FROM yypl_s
	GROUP BY intval_group;
QUIT;


/**【统计分析8- 不同间隔区间的业绩预告类型差异】**/
/** 以最后发布的为准 */
PROC SORT DATA = yypl_with_earning2 OUT = yypl_s;
	BY event_id descending f_date descending source;  
RUN;

PROC SORT DATA = yypl_s NODUPKEY OUT = yypl_s;
	BY event_id;
RUN;


PROC SQL;
	CREATE TABLE stat AS
	SELECT intval_group,
		count(distinct event_id) AS nobs,
		sum(have_forecast=0) AS nmiss,
		sum(have_forecast=1 AND e_type in ("预增","预盈","预升")) AS npos,
		sum(have_forecast=1 AND e_type in ("预减","预降","预亏")) AS nneg,
		sum(have_forecast=1 AND e_type in ("预平","预警")) AS neven,
		sum(have_forecast=1 AND missing(e_type)) AS nother
	FROM yypl_s
	GROUP BY intval_group;
QUIT;


/**【统计分析9- 仅针对1/1日之后才发布业绩预告的事件，统计不同间隔区间的业绩预告类型差异】**/
PROC SQL;
	CREATe TABLE yypl_s AS
	SELECT *
	FROM yypl_with_earning2
	WHERE event_id NOT IN (
		SELECT event_id 
		FROM yypl_with_earning2
		WHERE map_f_date<= pub_date AND have_forecast=1)
	AND have_forecast = 1;
QUIT;
/* 以第一条记录为准(想考虑是否合适作为退出时点)*/
PROC SORT DATA = yypl_s OUT = yypl_s;
	BY event_id f_date descending source;  
RUN;

PROC SORT DATA = yypl_s NODUPKEY OUT = yypl_s;
	BY event_id;
RUN;


PROC SQL;
	CREATE TABLE stat AS
	SELECT intval_group,
		count(distinct event_id) AS nobs,
		sum(have_forecast=1 AND e_type in ("预增","预盈","预升")) AS npos,
		sum(have_forecast=1 AND e_type in ("预减","预降","预亏")) AS nneg,
		sum(have_forecast=1 AND e_type in ("预平","预警")) AS neven,
		sum(have_forecast=1 AND missing(e_type)) AS nother
	FROM yypl_s
	GROUP BY intval_group;
QUIT;


