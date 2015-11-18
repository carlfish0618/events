/** ��������� */

/** �ʼǱ��汾 **/
%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\�޸İ汾; 


%LET input_dir = &product_dir.\input_data\employer_holding; 
%LET output_dir = &product_dir.\output_data\employer_holding;
LIBNAME product "&product_dir.\sasdata\employer_holding";


LIBNAME database "D:\Research\���ݿ�\ͨ��";

%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\Ȩ��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ϲ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\�¼��о�_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\������Ч��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\���Ӽ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ч����_ͨ�ú���.sas";

options validvarname=any; /* ֧�����ı����� */


/*** step0: �����¼��о�_�����ļ� **/
%LET env_start_date = 1jan2014;
%INCLUDE "&utils_dir.\�¼��о�_�����ļ�_���ذ汾.sas";

%LET adjust_start_date = 15dec2013;   
%LET adjust_end_date = 31jul2020;
/*%LET test_start_date = 1jan2014;   */
%LET test_end_date = 30oct2015;

%LET cut_date = 17jul2015;  /* �������� */
%LET stock_size = 0.0015; /** ����Ȩ�� */



/*** step1: ����ԭʼ���� **/
/*%read_from_excel(excel_path=&input_dir.\Ա���ֹ�_��������_20150916.xlsx,  output_table=employer_data, sheet_name = data2$);*/
%read_from_excel(excel_path=&input_dir.\Ա���ֹ�20151102.xlsx,  output_table=employer_data, sheet_name = data2$);

/*ÿ�ܵ�һ�������� */
%get_weekday_date(busday_table=database.busday, start_date=&adjust_start_date.,
	end_date=&adjust_end_date., rename=end_date, type=2, trade_day=1, output_table=week_date);
%move_date_offset(input_table=week_date, date_col=end_date,
	output_col=end_date6, offset=6, output_table=week_date, is_fill=0);
DATA week_date;
	SET week_date;
	IF missing(end_date6) THEN end_date6 = "&test_end_date."d; /* ��Ϊ�����������ڡ�*/
RUN;


/** Step2: ������������(����Ľ�����) */
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


/*������������*/
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



/** �����ռ��������������������� */
DATA employer_data;
	SET employer_data;
	buy_date = cal_buy_date;
	sell_date = cal_sell_date;
	DROP cal_buy_date cal_sell_date;
	event_id = _N_;
	IF buy_date >= "&cut_date."d;
RUN;



/** Step3: �ж��������������ʱ���Ƿ���������� */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.stock_code, B.end_date
	FROM 
	(SELECT distinct stock_code FROM employer_data) A,
	(SELECT end_date FROM week_date union SELECT end_date6 AS end_date FROM week_date) B  /* ���������������һ����β���� */
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


/** �ص�ʱ�򣬼������� */
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

	/** �����źŽ��и��� */
	/** �������� */
	IF not missing(event_id) THEN DO;
		IF r_hold = 1 THEN DO; /** �Ѿ����С�ֻ�ı��������ڣ����ı��������� */
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
	/** û�г������룬�ж��Ƿ���Ҫ���� */
	ELSE DO;
		IF r_hold = 1 AND end_date = r_sell_date THEN DO;  /* ���� */
			is_sell = 1;
			r_hold = 0;
			r_event_id = .;
			r_sell_date = .;
			r_buy_date = .;
		END;
	END;

	/** �������û�з��������������������ź� */
	hold = r_hold;
	sell_date = r_sell_date;
	buy_date = r_buy_date;
	event_id = r_event_id;
	
	FORMAT r_sell_date r_buy_date sell_date buy_date yymmdd10.;
	DROP r_sell_date r_buy_date r_event_id r_hold;
RUN;



/** �����޷����������������� */
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

	IF (rr_hold = 0 OR missing(rr_hold)) AND hold = 1  THEN DO ;   /* �����۳������ڣ���֮ǰһֱû�� */
		IF is_halt = 1 OR is_limit IN (1,3) THEN DO;
			f_hold = 0;  /* �޷����� */
			is_buy = 0;
			f_event_id = .;
		END;
		ELSE DO;
			f_hold = 1;
			is_buy = 1;
			f_event_id = event_id;
		END;
	END;
	/* ��Ҫ���� */
	ELSE IF rr_hold = 1 AND hold = 0 THEN DO; /** �����۳������⣬���ߵ������۵����� */
		IF is_halt = 1 OR is_limit IN (2,4) THEN DO; 
			f_hold = 1;  
			is_sell = 0;
			f_event_id = r_event_id;
		END;
		ELSE DO;
			f_hold = 0;
			is_sell = 1;
			f_event_id = r_event_id; /** �����Ա���event_id */
		END;
	END;
	ELSE IF rr_hold = 0 AND is_sell = 1 THEN DO;  /** ԭӦ������������Ϊͣ�Ƶ�ԭ��֮ǰһֱû�г��� */
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


/** Step5�����������¼�����������¼��������м���ն����ڲ�ȫ */
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

/* ���飺���������������ͬһ�졣���ˡ���β���������ܵ��� */
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



/*** Step4: ��ȡ�۸����ݣ������������ */
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

/* �������棺���뵱������=����/������ۣ�������������=����/ǰһ������ */
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


/*** Step5������������� **/

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
	last_accum_ret = accum_ret; /* ������һ�ڵ��ۼ����� */
	accum_ret = ((1+daily_ret/100)*(1+accum_ret/100)-1)*100;
RUN;

/** ����ÿֻ��Ʊ��ʼʱ�����stock_sizeȨ�أ���һֱ���е���������;��������λ */
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















