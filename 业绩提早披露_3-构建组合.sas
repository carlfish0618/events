/*** ������� **/
%LET event_start_date = 31dec2005;
%LET event_end_date = 31oct2015;

%LET test_start_date = 1jan2006;
%LET test_end_date = 31oct2015;
%LET stock_size = 0.0015;
%LET index_code = 000300;

%LET intval_group_cond = 1;

/*************** ���1�����飬�������ʼ����������Ʊ����� **/

/* ��actdate������������ */
%adjust_date_to_mapdate(rawdate_table=yypl_with_earning, mapdate_table=busday, raw_colname=actdate, 
		map_colname=date, output_table=yypl_s,is_backward=0, is_included=1);

/** ����map_date1��ǰһ�� */
%adjust_date_to_mapdate(rawdate_table=yypl_s, mapdate_table=busday, raw_colname=map_date1, 
		map_colname=date, output_table=yypl_s,is_backward=1, is_included=0);

/* �����գ�pub_date(��ÿ��1��1��) */
/* �˳��գ���ʽ�Ʊ��� */
PROC SQL;
	CREATE TABLE cross_sample AS
	SELECT stock_code AS stock_code LABEL "stock_code",
		report_period,
		pub_date AS event_date,
		intval_group,
		min(map_map_date1, map_actdate) AS date_end FORMAT yymmdd10.
	FROM yypl_s
	WHERE not missing(pub_date) AND "&event_start_date."d <= pub_date <= "&event_end_date."d
	AND not missing(map_actdate)
	AND intval_group in (&intval_group_cond.)
	ORDER BY stock_code, report_period, pub_date;
QUIT;
PROC SORT DATA = cross_sample NODUPKEY OUT = cross_sample;
	BY stock_code report_period;
RUN;

DATA cross_sample;
	SET cross_sample;
	event_id = _N_;
RUN;

/*************** ���2�����飬�������ʼ����������Ʊ����� **/

/*** ֻѡ����Щδ����ҵ��Ԥ������� */
PROC SQL;
	CREATe TABLE yypl_s AS
	SELECT *
	FROM yypl_with_earning2
	WHERE event_id NOT IN (
		SELECT event_id 
		FROM yypl_with_earning2
		WHERE map_f_date<= pub_date AND have_forecast=1);
QUIT;
/* �Ե�һ����¼Ϊ׼(�뿼���Ƿ������Ϊ�˳�ʱ��)*/
PROC SORT DATA = yypl_s OUT = yypl_s;
	BY event_id f_date descending source;  
RUN;

PROC SORT DATA = yypl_s NODUPKEY OUT = yypl_s;
	BY event_id;
RUN;

/* ��actdate������������ */
%adjust_date_to_mapdate(rawdate_table=yypl_s, mapdate_table=busday, raw_colname=actdate, 
		map_colname=date, output_table=yypl_s,is_backward=0, is_included=1);

/** ����map_date1��ǰһ�� */
%adjust_date_to_mapdate(rawdate_table=yypl_s, mapdate_table=busday, raw_colname=map_date1, 
		map_colname=date, output_table=yypl_s,is_backward=1, is_included=0);

/* �����գ�pub_date(��ÿ��1��1��) */
/* �˳��գ�min(��ʽ�Ʊ��գ�ԤԼ�գ�ҵ��Ԥ����) */
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
	AND intval_group in (&intval_group_cond.)
	ORDER BY stock_code, report_period, pub_date;
QUIT;
PROC SORT DATA = cross_sample NODUPKEY OUT = cross_sample;
	BY stock_code report_period;
RUN;


DATA cross_sample;
	SET cross_sample;
	event_id = _N_;
RUN;


/*********************************************** ���²����ʺ���������Ϲ��� ******************************************/

/** Step1: ȷ��������������� */
%event_gen_trade_date(event_table=cross_sample, buy_col=event_date, sell_col=date_end, 
							busday_table=busday, hq_table=hqinfo, output_table=cross_port);
/** ͳ���ж��������޷�����*/
PROC SQL;
	CREATE TABLE stat AS
	SELECT report_period, 
		count(1) AS nobs,
		sum(not missing(trade_buy_date)) AS nvalid,
		sum(trade_buy_date = event_date) AS nontime
	FROM cross_port
	GROUP BY report_period;
QUIT;

/** Step2: �޳��޷���������������� */
DATA cross_port;
	SET cross_port;
	IF not missing(trade_buy_date);
RUN;

/** Step4����Ϲ��� */
%event_gen_fixed_windows(event_table=cross_port, start_col=trade_buy_date, end_col=trade_sell_date, 
		busday_table=busday, output_table=cross_port);
PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT win_date AS end_date,
		stock_code,
	CASE 
		WHEN win_date = trade_sell_date THEN 1
		ELSE 0
	END AS is_sell,
	CASE 
		WHEN win_date = trade_buy_date THEN 1
		ELSE 0
	END AS is_buy,
	event_id,
	trade_buy_date,
	trade_sell_date
	FROM cross_port
	ORDER BY stock_code, end_date, trade_buy_date, trade_sell_date;
QUIT;
PROC SORT DATA = test_stock_pool NODUPKEY;
	BY stock_code end_date;
RUN;

/*** Step4: ��ȡ�۸����ݣ������������ */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, coalesce(B.factor*B.value/B.vol, B.factor*B.close) AS price_avr, 
		coalesce(B.factor*B.close) AS price_close
	FROM test_stock_pool A LEFt JOIN hqinfo B
	ON A.end_date = B.end_date AND A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA test_stock_pool;
	SET tmp;
RUN;


%get_nearby_data(input_table=test_stock_pool, identity=stock_code,
	raw_col=price_avr, date_col=end_date, output_col=pre_price_avr, offset=-1, 
	output_table=test_stock_pool);

%get_nearby_data(input_table=test_stock_pool, identity=stock_code,
	raw_col=price_close, date_col=end_date, output_col=pre_price_close, offset=-1, 
	output_table=test_stock_pool);

/* �������棺���뵱������=����/������ۣ�������������=����/ǰһ������ */
DATA test_stock_pool;
	SET test_stock_pool;
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

/*** Step5������������� **/

PROC SORT DATA = test_stock_pool;
	BY stock_code end_date;
RUN;
DATA  test_stock_pool;
	SET  test_stock_pool;
	BY stock_code;
	RETAIN accum_ret 0;
	RETAIN last_accum_ret 0;

	IF first.stock_code OR is_buy = 1 THEN DO;
		accum_ret = 0;
	END;
	last_accum_ret = accum_ret; /* ������һ�ڵ��ۼ����� */
	accum_ret = ((1+daily_ret/100)*(1+accum_ret/100)-1)*100;
RUN;


/** ����ÿֻ��Ʊ��ʼʱ�����stock_sizeȨ�أ���һֱ���е���������;��������λ */
PROC SQL;
	CREATE TABLE test_summary AS
	SELECT end_date, 
	(sum(&stock_size.*(1+accum_ret/100))/sum(&stock_size.*(1+last_accum_ret/100))-1)*100 AS daily_ret,
	count(distinct stock_code) AS nstock,
	sum(is_buy=1) AS n_buy,
	sum(is_sell=1) AS n_sell,
	sum(is_buy=0 and is_sell=0) AS n_holding
	FROM  test_stock_pool
	GROUP BY end_date;
QUIT;

/** �������� */
%get_daily_date(busday_table=busday, start_date=&test_start_date., end_date=&test_end_date., 
	rename=end_date, output_table=daily_busdate);
PROC SQL;
	CREATE TABLE daily_port AS
	SELECT A.end_date AS date,
		coalesce(daily_ret,0) AS daily_ret,
		coalesce(nstock, 0) AS nstock,
		coalesce(n_buy,0) AS n_buy,
		coalesce(n_sell,0) AS n_sell,
		coalesce(n_holding,0) AS n_holding
	FROM daily_busdate A LEFT JOIN test_summary B
	ON A.end_date = B.end_date;
QUIT;

DATA daily_port;
	SET daily_port;
	RETAIN index 1000;
	index = index * (1+daily_ret/100);
RUN;

/** ֻ����nstock>0����������ȡ��׼���бȽ� */
PROC SQL;
	CREATE TABLE bm_index AS
	SELECT A.end_date AS date, 
		(A.close/A.pre_close-1)*100 AS daily_ret,
		(A.close/A.pre_close-1)*100 * B.nstock * &stock_size.AS daily_bps
	FROM index_hqinfo A LEFT JOIN daily_port B
	ON A.end_date = B.date AND B.nstock > 0
	WHERE not missing(B.date)
	AND stock_code = "&index_code.";
QUIT; 
PROC SQL;
	CREATE TABLE daily_port_s AS
	SELECT *, daily_ret*nstock*&stock_size. AS daily_bps
	FROM daily_port
	WHERE nstock >0;
QUIT;

/** �����ǲ�λӰ�� */
%eval_pfmance(index_pool=daily_port_s, bm_pool=bm_index, index_ret=daily_ret, bm_ret=daily_ret, 
		start_date=&test_start_date., end_date=&test_end_date., type=1, output_table=daily_result);
/** ���ǲ�λӰ�� */
%eval_pfmance(index_pool=daily_port_s, bm_pool=bm_index, index_ret=daily_bps, bm_ret=daily_bps, 
		start_date=&test_start_date., end_date=&test_end_date., type=1, output_table=daily_result_bps);
