/**高送转统计分析　**/

/*** 策略开始时间为：2006/1/1 ****/
/** 外部数据库：
(1) event_db.fh: 预约披露表
(2) event_db.earning_actual: 净利润表
(3) event_db.zcfzb 资产负债表
***/

%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 

%LET input_dir = &product_dir.\input_data\高送转; 
%LET output_dir = &product_dir.\output_data\高送转;
LIBNAME product "&product_dir.\sasdata\高送转";

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


/*** step0: 调用事件研究_配置文件 **/
%LET env_start_date = 30jun2005;
%LET index_code = 000300;
%INCLUDE "&utils_dir.\事件研究_配置文件_本地版本_适合版本2.sas";


/******** 高送转事件基本属性 ********/
/** 1-分红表：
	(1) stock_code
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
	SET event_db.fh;
	sz_fz = coalesce(sg_fz,0)+coalesce(zz_fz,0);
	year = floor(yaggr/10000);
	period = floor((fh_year-floor(fh_year/10000)*10000)/100);
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
	SELECT A.year, A.stock_code, A.adj_yaggr_date AS yaggr, A.sz_fz,
		B.pre_close,
		coalesce(A.px_fzsh/B.pre_close/10,0) AS px_rate,
		A.fh_year,
		month(adj_yaggr_date) AS month,
		year(adj_yaggr_date) AS year2,
		period
	FROM fh A LEFT JOIN hqinfo B
	ON A.stock_code = B.stock_code AND A.adj_yaggr_date = B.end_date
	ORDER BY year, stock_code, yaggr;
QUIT;

DATA fh;
	SET tmp;
RUN;
/** 以0.005分档 */
PROC SQL;
	CREATE TABLE fh_px_class AS
	SELECT year, sum(px_rate>0) AS  nobs_sz,
		sum(floor(px_rate/0.005)>=2) AS nobs_gpx, /* 高派现，定义为分红收益率1% */
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



/*** Step2：预案期样本数量分布*/
PROC SQL;
	CREATE TABLE stat AS
	SELECT fh_year, 
		count(1) AS nobs,
		sum(sz_fz>0) AS nobs1,
		sum(px_rate>0) AS nobs2,
		sum(floor(sz_fz)>=10) AS gsz_nobs,
		sum(floor(px_rate/0.005)>=2) AS gpx_nobs
	FROM fh
	GROUP BY fh_year;
QUIT;

/*** Step3: 预案时间分布 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT month, 
		sum(sz_fz>0) AS nobs1,
		sum(sz_fz>0 AND period=6) AS nobs_zb,
		sum(sz_fz>0 AND period=12) AS nobs_nb
	FROM fh
	WHERE sz_fz >= 10
	GROUP BY month;
QUIT;

PROC SQL;
	CREATE TABLE stat AS
	SELECT year2, month, 
		sum(sz_fz>0) AS nobs1,
		sum(sz_fz>=10) AS nobs2
	FROM fh
	GROUP BY year2, month;
QUIT;

