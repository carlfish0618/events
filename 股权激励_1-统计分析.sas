/*** 股权激励 **/
/** 定向增发：统计分析 */
/*** 策略开始时间为：2006/1/1 ****/
/** 外部数据库：
(1) event_db.ashareincdescription: 股权激励
***/


%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 

%LET input_dir = &product_dir.\input_data\股权激励; 
%LET output_dir = &product_dir.\output_data\股权激励;
LIBNAME product "&product_dir.\sasdata\股权激励";

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
%LET index_code = 000300;
%INCLUDE "&utils_dir.\事件研究_配置文件_本地版本_适合版本2.sas";


/**** Step1: 样本分布分析 **/
DATA gqjl;
	SET event_db.ashareincdescription;
	LENGTH stock_code $6.;
	stock_code = substr(s_info_windcode,1,6);
	yaggr = input(preplan_ann_date, yymmdd8.);
	gddhr = input(gm_date, yymmdd8.);
	ssggr = input(implement_date, yymmdd8.);
	qsr = input(s_inc_firstinc, yymmdd10.);
	FORMAT qsr yaggr gddhr ssggr yymmdd10.;
	year = year(yaggr);
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT year, count(1) AS nobs,
		sum(s_inc_subject=1) AS nobs1,
		sum(s_inc_subject=2) AS nobs2,
		sum(s_inc_subject=3) AS nobs3
	FROM gqjl
	GROUP BY year;
QUIT;

PROC SQL;
	CREATE TABLE stat AS
	SELECT s_inc_subject, s_inc_type, count(1) AS nobs
	FROM gqjl
	GROUP BY s_inc_subject, s_inc_type;
QUIT;

/**** Step2: 三个关键时间点的分布 **/
%LET time_var = qsr;
DATA tt;
	SET gqjl;
	month = month(&time_var.);
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT month, count(1) AS nobs
	FROM tt
	GROUP BY month;
QUIT;

/** Step3: 关键时间点之间的间隔 */
%LET dif_var = dif2;
DATA tt;
	SET gqjl;
	dif1 = gddhr - yaggr;
	dif2 = ssggr - gddhr;
	dif3 = qsr - ssggr;
	mark = 1;
RUN;
%cal_dist(input_table=tt, by_var=mark, cal_var=&dif_var., out_table=stat, pctlpts=100 75 50 25 0);


/** Step4: 进度安排 */
DATA tt;
	SET gqjl;
	IF missing(gddhr);
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT progress, count(1) AS nobs
	FROM tt
	GROUP BY progress;
QUIT;
