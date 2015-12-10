/*** 股东高管二级市场增减持 **/

/** 外部文件(来自wind)：
	定向增发发行资料_截至20151208.xlsx
	定向增发预案_截至20151208.xlsx
	增发实施_截至20151208.xlsx
***/

/** 文件字段： 
(1) 定向增发预案：
代码
名称
预案公告日
方案进度
发行方式
增发价格
预案价下限
最新价
预案差价率(%)
倒挂
定价规则
增发数量(万股)
预计募集资金(亿元)
定向增发目的
定向增发说明
发行对象
发行对象说明
认购方式
增发类型
股东大会公告日
发审委通过公告日
证监会批准公告日
预测净利润(万元)
主承销商
财务顾问
预测年度
证监会行业(2012版)
证监会行业
Wind行业
证券类型
(2) 定向增发发行资料
代码
名称
增发公告日
定价基准日
增发价格
自发行价涨跌幅(后复权)
预案价下限
增发数量(万股)
预计募集资金(亿元)
实际募资总额(亿元)
发行费用(亿元)
实际募资净额(亿元)
发行费率(%)
定向增发目的
定向增发目的说明
发行对象
增发类型
发行折价率(%)
大股东是否参与认购
大股东认购比例(%)
大股东认购金额(亿元)
大股东认购方式
增发价格制定依据
定向基准日类型
实施价相对基准价格比例(%)
预案价相对基准价格比例(%)
承销方式
主承销商
财务顾问
预案公告日
股东大会公告日
发审委通过公告日
证监会核准公告日
发行日期
股权登记日
定增股份变动日
限售股份解禁日
企业性质
证监会行业(2012版)
证监会行业
Wind行业
公告链接
(3) 增发实施
代码
名称
发行代码
增发公告日
发行日期
发行方式
发行价格
增发日收盘价
最新收盘价
昨收盘价
自发行价涨跌幅(% 后复权)
增发数量(万股)
预计募集资金(亿元)
实际募资总额(亿元)
发行费用(亿元)
实际募资净额(亿元)
发行对象
认购方式
增发类型
承销方式
主承销商
财务顾问
限售股解禁时间
证监会行业(2012版)
证监会行业
Wind行业
证券类型	
***/



%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 

%LET input_dir = &product_dir.\input_data\增减持; 
%LET output_dir = &product_dir.\output_data\增减持;
LIBNAME product "&product_dir.\sasdata\增减持";

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

/**** Step1: 从外部读入数据 **/
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2005.xlsx, output_table=product.d2005, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2006.xlsx, output_table=product.d2006, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2007.xlsx, output_table=product.d2007, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2008.xlsx, output_table=product.d2008, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2009.xlsx, output_table=product.d2009, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2010.xlsx, output_table=product.d2010, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2011.xlsx, output_table=product.d2011, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2012.xlsx, output_table=product.d2012, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2013.xlsx, output_table=product.d2013, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2014.xlsx, output_table=product.d2014, sheet_name = Wind资讯$);
%read_from_excel(excel_path=&input_dir.\重要股东二级市场交易_2015.xlsx, output_table=product.d2015, sheet_name = Wind资讯$);

/*** Step2：对数据字段进行处理 */
%MACRO modify_zjc_data(input_data, output_data);
	DATA &output_data.;
		SET &input_data.;
		IF trim(left(代码)) = "数据来源：Wind资讯" THEN delete;
		IF missing(代码) THEN delete;
		LENGTH stock_code $6.;
		LENGTH stock_name $20.;
		LENGTH holder_name $100.;
		LENGTH holder_type $10.;
		LENGTH relation $20.;
		LENGTH direction $10.;
		stock_code = substr(代码,1,6);
		stock_name = trim(left(名称));
		pub_date = 公告日期;
		start_change_date = 变动起始日期;
		start_end_date = 变动截止日期;
		holder_name = 股东名称;
		holder_type = 股东类型;
		relation = 股东与高管关系;
		direction = 方向;
		shares = '变动数量(万股)'n;
		big_shares = '通过大宗交易系统的变动数量(万股)'n;
		free_pct = '变动数量占流通股比(%)'n;
		total_pct = '变动数量占总股本比例(%)'n;
		after_free_shares = '变动后持有流通股数量(万股)'n;
		after_free_pct = '变动后流通股数量占流通股比(%)'n;
		after_total_shares = '变动后持股总数(万股)'n;
		after_total_pct = '变动后持股比例(%)'n;
		trade_avg_price = 交易平均价;
		avg_price = 变动期间股票均价;
		value = '变动部分参考市值(万元)'n;
		is_lock = 是否为减持解禁股份;
		FORMAT pub_date start_change_date start_end_date yymmdd10.;
		DROP 代码
		名称
		公告日期
		变动起始日期
		变动截止日期
		股东名称
		股东类型
		关联高管
		股东与高管关系
		方向
		'变动数量(万股)'n
		'通过大宗交易系统的变动数量(万股)'n
		'变动数量占流通股比(%)'n
		'变动数量占总股本比例(%)'n
		'变动后持有流通股数量(万股)'n
		'变动后流通股数量占流通股比(%)'n
		'变动后持股总数(万股)'n
		'变动后持股比例(%)'n
		交易平均价
		变动期间股票均价
		'变动部分参考市值(万元)'n
		币种
		是否为减持解禁股份
		说明
		证监会行业
		Wind行业;
	RUN;
%MEND modify_zjc_data;
%modify_zjc_data(product.d2005, d2005);
%modify_zjc_data(product.d2006, d2006);
%modify_zjc_data(product.d2007, d2007);
%modify_zjc_data(product.d2008, d2008);
%modify_zjc_data(product.d2009, d2009);
%modify_zjc_data(product.d2010, d2010);
%modify_zjc_data(product.d2011, d2011);
%modify_zjc_data(product.d2012, d2012);
%modify_zjc_data(product.d2013, d2013);
%modify_zjc_data(product.d2014, d2014);
%modify_zjc_data(product.d2015, d2015);
DATA zjc;
	SET d20:;
RUN;

PROC DATASETS lib=work  NOLISt;
   DELETE d20: /MEMTYPE=data;
QUIT;


/**** Step1: 样本分布分析 **/
DATA zjc;
	SET zjc;
	year = year(pub_date);
	month = month(pub_date);
	IF missing(pub_date) THEN delete;
RUN;



/**基于股票数量*/
PROC SQL;
	CREATE TABLE stat AS
	SELECT year, count(distinct stock_code) AS nobs,
		count(distinct input(stock_code,8.)*(holder_type="高管"))-1 AS nobs1,
		count(distinct input(stock_code,8.)*(holder_type="公司"))-1 AS nobs2,
		count(distinct input(stock_code,8.)*(holder_type="个人"))-1 AS nobs3
	FROM zjc
	WHERE direction="增持" AND not missing(big_shares)
	GROUP BY year;
QUIT;



/*** 基于涉及的次数 */
PROC SQL;
	CREATE TABLE zjc_nodup AS
	SELECT stock_code, pub_date, direction, year, month, count(1) AS nobs
	FROM zjc
	WHERE holder_type = "高管" AND not missing(big_shares)
	GROUP BY stock_code, pub_date, direction, year, month;
QUIT;
PROc SQL;
	CREATE TABLE stat AS
	SELECT year, 
		sum(direction="增持") AS nzc,
		sum(direction="减持") AS njc
	FROM zjc_nodup
	GROUP BY year;
QUIT;
data tt;
	set zjc;
	if holder_type = "高管" AND missing(relation);
run;

