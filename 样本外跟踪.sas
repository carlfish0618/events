/** 样本外跟踪 */

/** 笔记本版本 **/
%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 


%LET input_dir = &product_dir.\input_data\employer_holding; 
%LET output_dir = &product_dir.\output_data\employer_holding;
LIBNAME product "&product_dir.\sasdata\employer_holding";


LIBNAME database "D:\Research\数据库\通用";

%INCLUDE "&utils_dir.\日期_通用函数.sas";
%INCLUDE "&utils_dir.\交易_通用函数.sas";
%INCLUDE "&utils_dir.\权重_通用函数.sas";
%INCLUDE "&utils_dir.\组合构建_通用函数.sas";
%INCLUDE "&utils_dir.\其他_通用函数.sas";
%INCLUDE "&utils_dir.\事件研究_通用函数.sas";
%INCLUDE "&utils_dir.\因子有效性_通用函数.sas";
%INCLUDE "&utils_dir.\计量_通用函数.sas";
%INCLUDE "&utils_dir.\因子计算_通用函数.sas";
%INCLUDE "&utils_dir.\绩效评估_通用函数.sas";

options validvarname=any; /* 支持中文变量名 */


/*** step0: 调用事件研究_配置文件 **/
%LET env_start_date = 1jan2014;
%INCLUDE "&utils_dir.\事件研究_配置文件_本地版本.sas";

%LET adjust_start_date = 15dec2013;   
%LET adjust_end_date = 31jul2020;
/*%LET test_start_date = 1jan2014;   */
%LET test_end_date = 30oct2015;

%LET cut_date = 17jul2015;  /* 回溯日期 */
%LET stock_size = 0.0015; /** 个股权重 */



/*** step1: 读入原始数据 **/
/*%read_from_excel(excel_path=&input_dir.\员工持股_样本回溯_20150916.xlsx,  output_table=employer_data, sheet_name = data2$);*/
%read_from_excel(excel_path=&input_dir.\员工持股20151102.xlsx,  output_table=employer_data, sheet_name = data2$);

/*每周第一个交易日 */
%get_weekday_date(busday_table=database.busday, start_date=&adjust_start_date.,
	end_date=&adjust_end_date., rename=end_date, type=2, trade_day=1, output_table=week_date);
%move_date_offset(input_table=week_date, date_col=end_date,
	output_col=end_date6, offset=6, output_table=week_date, is_fill=0);
DATA week_date;
	SET week_date;
	IF missing(end_date6) THEN end_date6 = "&test_end_date."d; /* 认为是卖出的日期。*/
RUN;


/** Step2: 计算买入日期(最近的交易日) */
%adjust_date_to_mapdate(rawdate_table=employer_data, mapdate_table=week_date, 
	raw_colname=end_date, map_colname=end_date, output_table=employer_data,
	is_backward=0, is_included=1);
DATA employer_data;
	SET employer_data;
	RENAME map_end_date = cal_buy_date;
	LABEL cal_buy_date ="cal_buy_date";
RUN;
data tt;
	set employer_data;
	if buy_date ~= cal_buy_date;
run;


/*计算卖出日期*/
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.end_date6 AS cal_sell_date LABEL "cal_sell_date"
	FROM employer_data A LEFT JOIN week_date B
	ON A.cal_buy_date = B.end_date
	ORDER BY A.end_date, A.stock_code, A.cal_buy_date;
QUIT;

DATA employer_data;
	SET tmp;
RUN;
PROC SORT DATA = employer_data NODUPKEY;
	BY end_date stock_code cal_buy_date;
RUN;



/** 用最终计算出来的买入和卖出日期 */
DATA employer_data;
	SET employer_data;
	buy_date = cal_buy_date;
	sell_date = cal_sell_date;
	DROP cal_buy_date cal_sell_date;
	event_id = _N_;
	IF buy_date >= "&cut_date."d;
RUN;



/** Step3: 判断在买入或者卖出时点是否有特殊情况 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.stock_code, B.end_date
	FROM 
	(SELECT distinct stock_code FROM employer_data) A,
	(SELECT end_date FROM week_date union SELECT end_date6 AS end_date FROM week_date) B  /* 这样才能纳入最后一个截尾日期 */
	ORDER BY end_date, stock_code;
QUIT;
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, B.event_id, B.sell_date, B.buy_date
	FROM tmp A LEFT JOIN employer_data B
	ON A.stock_code = B.stock_code AND A.end_date = B.buy_date
	ORDER BY stock_code, end_date;
QUIT;
DATA employer_data_expand;
	SET tmp2;
RUN;
/*data tt;*/
/*	set employer_data_expand;*/
/*	if buy_date = sell_date and not missing(sell_date);*/
/*run;*/


/** 重叠时候，继续持有 */
DATA employer_data_expand;
	SET employer_data_expand;
	BY stock_code;
	RETAIN r_sell_date .;
	RETAIN r_buy_date .;
	RETAIN r_event_id .;
	RETAIN r_hold  0;
	is_buy = 0;
	is_sell = 0;
	IF first.stock_code THEN DO;
		r_sell_date = .;
		r_buy_date = .;
		r_event_id = .;
		r_hold =  0;
	END;

	/** 今天信号进行更新 */
	/** 出现买入 */
	IF not missing(event_id) THEN DO;
		IF r_hold = 1 THEN DO; /** 已经持有。只改变卖出日期，不改变买入日期 */
			r_event_id = event_id;
			r_sell_date = sell_date;
		END;
		ELSE DO;
			r_hold = 1;
			r_event_id = event_id;
			r_sell_date = sell_date;
			r_buy_date = buy_date;
			is_buy = 1;
		END;	
	END;
	/** 没有出现买入，判断是否需要卖出 */
	ELSE DO;
		IF r_hold = 1 AND end_date = r_sell_date THEN DO;  /* 卖出 */
			is_sell = 1;
			r_hold = 0;
			r_event_id = .;
			r_sell_date = .;
			r_buy_date = .;
		END;
	END;

	/** 如果今天没有发生变更，则延续昨天的信号 */
	hold = r_hold;
	sell_date = r_sell_date;
	buy_date = r_buy_date;
	event_id = r_event_id;
	
	FORMAT r_sell_date r_buy_date sell_date buy_date yymmdd10.;
	DROP r_sell_date r_buy_date r_event_id r_hold;
RUN;



/** 考虑无法买入或者卖出的情况 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.is_halt, B.is_limit
	FROM employer_data_expand A LEFT JOIN market_table B
	ON A.end_date = B.end_date AND A.stock_code=B.stock_code
	ORDER BY A.stock_code, A.end_date;
QUIT;

DATA employer_data_expand;
	SET tmp;
	BY stock_code;
	RETAIN rr_hold .;
	RETAIN r_event_id .;
	
	IF first.stock_code THEN DO;
		rr_hold = .;
		r_event_id = .;
	END;

	IF (rr_hold = 0 OR missing(rr_hold)) AND hold = 1  THEN DO ;   /* 在理论持有期内，且之前一直没买 */
		IF is_halt = 1 OR is_limit IN (1,3) THEN DO;
			f_hold = 0;  /* 无法买入 */
			is_buy = 0;
			f_event_id = .;
		END;
		ELSE DO;
			f_hold = 1;
			is_buy = 1;
			f_event_id = event_id;
		END;
	END;
	/* 需要卖出 */
	ELSE IF rr_hold = 1 AND hold = 0 THEN DO; /** 在理论持有期外，或者到了理论的卖点 */
		IF is_halt = 1 OR is_limit IN (2,4) THEN DO; 
			f_hold = 1;  
			is_sell = 0;
			f_event_id = r_event_id;
		END;
		ELSE DO;
			f_hold = 0;
			is_sell = 1;
			f_event_id = r_event_id; /** 卖出仍保留event_id */
		END;
	END;
	ELSE IF rr_hold = 0 AND is_sell = 1 THEN DO;  /** 原应该卖出，但因为停牌等原因之前一直没有持有 */
		is_sell = 0;
	END;
	ELSE DO;
		f_hold = hold;
		f_event_id = event_id;
	END;
	rr_hold = f_hold;
	r_event_id = f_event_id;
RUN;
/*DATA tt;*/
/*	SET employer_data_expand;*/
/*	If f_hold =1 OR is_sell = 1;*/
/*	IF missing(f_event_id);*/
/*RUN;*/

	
DATA employer_data_expand;
	SET employer_data_expand;
	IF f_hold = 1 OR is_sell = 1;
	DROP  rr_hold is_halt is_limit r_event_id;
RUN;


/** Step5：重新整理事件买入和卖出事件，并将中间的日度日期补全 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT f_event_id AS event_id, stock_code,
		max(end_date) AS sell_date FORMAT yymmdd10.,
		min(end_date) AS buy_date FORMAT yymmdd10.
	FROM employer_data_expand
	GROUP BY f_event_id, stock_code;
QUIT;
DATA employer_data_update;
	SET tmp;
RUN;

/* 检验：买入和卖出不会是同一天。除了“截尾”操作可能导致 */
DATA tt;
	SET employer_data_update;
	IF buy_date = sell_date;
RUN;


%get_daily_date(busday_table=database.busday, start_date=&adjust_start_date.,
	end_date=&adjust_end_date., rename=end_date, output_table=daily_date);

PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.end_date
	FROM employer_data_update A LEFT JOIN daily_date B
	ON B.end_date <= A.sell_date AND B.end_date >= A.buy_date
	ORDER BY A.event_id, B.end_date;
QUIT;
DATA employer_data_update;
	SET tmp;
	IF end_date = buy_date THEN is_buy = 1;
	ELSE is_buy = 0;
	IF end_date = sell_date THEN is_sell = 1;
	ELSE is_sell = 0;
RUN;



/*** Step4: 提取价格数据，计算个股收益 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, coalesce(B.factor*B.value/B.vol, B.factor*B.close) AS price_avr, 
		coalesce(B.factor*B.close) AS price_close
	FROM employer_data_update A LEFt JOIN hqinfo B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA employer_data_update;
	SET tmp;
RUN;


%get_nearby_data(input_table=employer_data_update, identity=stock_code,
	raw_col=price_avr, date_col=end_date, output_col=pre_price_avr, offset=-1, 
	output_table=employer_data_update);

%get_nearby_data(input_table=employer_data_update, identity=stock_code,
	raw_col=price_close, date_col=end_date, output_col=pre_price_close, offset=-1, 
	output_table=employer_data_update);

/* 计算收益：买入当天收益=收盘/当天均价，卖出当天收益=均价/前一天收盘 */
DATA employer_data_update;
	SET employer_data_update;
	IF is_buy = 1 THEN DO;
		daily_ret = (price_close/price_avr-1)*100;
	END;
	ELSE IF is_sell = 1 THEN DO;
		daily_ret = (price_avr/pre_price_close-1)*100;
	END;
	ELSE DO;
		daily_ret = (price_close/pre_price_close-1)*100;
	END;
RUN;


/*** Step5：计算组合收益 **/

PROC SORT DATA = employer_data_update;
	BY stock_code end_date;
RUN;
DATA employer_data_update;
	SET employer_data_update;
	BY stock_code;
	RETAIN accum_ret 0;
	RETAIN last_accum_ret 0;

	IF first.stock_code OR is_buy = 1 THEN DO;
		accum_ret = 0;
	END;
	last_accum_ret = accum_ret; /* 保存上一期的累计收益 */
	accum_ret = ((1+daily_ret/100)*(1+accum_ret/100)-1)*100;
RUN;

/** 假设每只股票开始时候分配stock_size权重，并一直持有到卖出。中途不调整仓位 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT end_date, 
	(sum(&stock_size.*(1+accum_ret/100))/sum(&stock_size.*(1+last_accum_ret/100))-1)*100 AS daily_ret,
	count(distinct stock_code) AS nstock,
	sum(is_buy=1) AS n_buy,
	sum(is_sell=1) AS n_sell,
	sum(is_buy=0 and is_sell=0) AS n_holding
	FROM employer_data_update
	GROUP BY end_date;
QUIT;
DATA index;
	SET tmp;
	RETAIN index 1000;
	index = index * (1+daily_ret/100);
RUN;


libname myxls "D:\busday.xlsx";
DATA myxls.busday;
	SET database.busday;
RUN;
libname myxls clear;















