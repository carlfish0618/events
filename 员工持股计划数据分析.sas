/*** Ա���ֹɼƻ����ݷ��� **/
/** �����ֶΰ�����
(1) have_condition: �Ƿ�����������
(2) report_date: ��������
(3) cover_num: ��������
(4) senior_num: �߹�����
(5) money: ����Ԫ��
(6) lock_month: ������
(7) c_resource: �ʽ���Դ
(8) s_resource: ��Ʊ��Դ
(9) is_structure: �Ƿ�ṹ��
********/


%LET product_dir = D:\Research\Data\Ա���ֹɼƻ�\Ա���ֹɼƻ�;
%LET input_dir = &product_dir.\input_data\employer_holding; 
%LET output_dir = &product_dir.\output_data\employer_holding;
LIBNAME product "&product_dir.\sasdata\employer_holding";

%LET utils_dir = D:\Research\Data\Ա���ֹɼƻ�\Ա���ֹɼƻ�\�޸İ汾; 
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\Ȩ��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ϲ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\�¼��о�_ͨ�ú���.sas";

options validvarname=any; /* ֧�����ı����� */

/*** step1: ����ԭʼ����(���ñ���) **/
%read_from_excel(excel_path=&input_dir.\Ա���ֹɼƻ�����.xlsx,  output_table=product.raw_data, sheet_name = Sheet1$);

/** Step2: ������������ */
%adjust_date_modify(busday_table=busday , raw_table=product.raw_data ,colname=report_date,  output_table=raw_data, is_forward = 1 )

/** ͳ�ƣ���ĩ������������ */
/** 21����������ĩ������Ϣ��97���ڽ����շ�����Ϣ */
PROC SQL;
	CREATE TABLE stat AS
	SELECT report_date_is_busday, count(1) AS nobs
	FROM raw_data
	GROUP BY report_date_is_busday;
QUIT;


/** Step3: ��ȡ�Ӽ������¼��� */
DATA event_table(keep = stock_code event_id event_date);
	SET raw_data;
	event_id = _N_;
	event_date = adj_report_date;
	FORMAT event_date yymmdd10.;
RUN;
PROC SORT DATA = event_table NODUPKEY;
	BY event_date stock_code;
RUN;


/** Step4: ���˲����¼� */
%filter_event(event_table, stock_info_table, market_table, event_table2, ndays = 365, halt_days = 1, is_filter_mark = 1);

/* ɸѡ�������˵��¼� */
/** 300376������ʱ��δ��1�� */
/** ��6ֻ��Ʊ����������ͣ�ơ�����3ֻͣ�Ƴ���20�������գ�3ֻ����һֻͣ�Ƴ���60��������) */
DATA filter_event;
	SET event_table2;
	IF filter >0;
RUN;

/** ���գ�ֻ�޳�����δ��1��Ĺ�Ʊ **/
%filter_event(event_table, stock_info_table, market_table, event_table2, ndays = 365, halt_days = 10000, is_filter_mark = 0);

/** Step5: ȡ���ɺͻ�׼�۸����ݣ�����֮���¼�������ʹ�� ***/
/** ���ɼ۸����� */
PROC SQL;
	CREATE TABLE stock_hqinfo_subset AS
	SELECT end_date, 
	stock_code, close AS price LABEL "price", 
	pre_close AS last_price LABEL "last_price"
	FROM hqinfo
	ORDER BY end_date, stock_code;
QUIT;

/* ��׼�۸����ݣ�ÿ����Ʊ�����и��ԵĻ�׼��*/
PROC TRANSPOSE DATA = index_hqinfo OUT = index_hqinfo_close(drop = _NAME_ _LABEL_) prefix = index_;
	ID stock_code;
	VAR close;
	BY end_date;
RUN;

PROC TRANSPOSE DATA = index_hqinfo OUT = index_hqinfo_preclose(drop = _NAME_ _LABEL_) prefix = index_;
	ID stock_code;
	VAR pre_close;
	BY end_date;
RUN;

PROC SQL;
	CREATE TABLE index_hqinfo_subset AS
	SELECT A.end_date, A.stock_code, 
		CASE WHEN b.bk = "����" THEN C.index_000300
			WHEN b.bk = "��С��ҵ��" THEN C.index_399101
			WHEN b.bk = "��ҵ��" THEN C.index_399102
			ELSE . END AS price,
		CASE WHEN b.bk = "����" THEN D.index_000300
			WHEN b.bk = "��С��ҵ��" THEN D.index_399101
			WHEN b.bk = "��ҵ��" THEN D.index_399102
			ELSE . END AS last_price
	FROM hqinfo A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code
	LEFT JOIN index_hqinfo_close C
	ON A.end_date = C.end_date
	LEFT JOIN index_hqinfo_preclose D
	ON A.end_date = D.end_date
	ORDER BY A.end_date, A.stock_code;
QUIT;


/*************** Step6-test: ���������ԣ� �¼��о���ܣ����Թ��ò��裬ֱ�Ӳ���֮���ģ�黯����) */

%LET event_name = hold;
/*%gen_overlapped_win(eventName=hold, event_table=event_table2, stock_hqinfo_table=stock_hqinfo_subset, */
/*	bm_hqinfo_table=index_hqinfo_subset, start_win=-60, end_win=60, busday_table = busday);*/

%gen_no_overlapped_win(eventName=&event_name., event_table=event_table2, stock_hqinfo_table=stock_hqinfo_subset, 
	bm_hqinfo_table=index_hqinfo_subset, start_win=-60, end_win=60);

%mark_event_win(event_win_table=&event_name._hq, stock_info_table=stock_info_table, market_table = market_table, 
	output_table = &event_name._hq);	
/* ��ͬʱ�䴰���£������г�����ķֲ� */
%mark_stat(event_win_table = &event_name._hq, output_table = &event_name._hq_stat);

/** �����г�����ķֲ� */
/** 878����������ͣ�ơ�27������һ����ͣ�ġ�22�����ڷǸ����գ�һ����ͣ�� */
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT mark, count(1) AS nobs*/
/*	FROM &event_name._hq*/
/*	GROUP BY mark;*/
/*QUIT;*/

%cal_win_ret(eventName=hold, event_hq_table=product.hold_hq, buy_win=0,  
		filter_invalid_after=1, filter_invalid_before=1, 
		filter_limit_after = 1, filter_limit_before = 1);

/** ����Ƿ���alpha����retȱʧ����� */
/* ������������� */
/*DATA tt;*/
/*	SET &eventName._alpha;*/
/*	IF test_mark = 0;*/
/*RUN;*/

/** ���filter_invalid/filter_limit��Ӱ���Ƿ���ȷ */
/*DATA tt;*/
/*	SET &eventName._alpha;*/
/*	IF mark =1;*/
/*RUN;*/
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT win, sum(not missing(alpha_mdf)) AS nobs,*/
/*		count(1) AS nobs_f*/
/*	FROM &eventName._alpha*/
/*	GROUP BY win;*/
/*QUIT;*/

/** �¼�ǰ���� */
/*%append_ahead_effect(alpha_table=&eventName._alpha, win_ahead=-10, alpha_table_edit=&eventName._alpha2);*/
DATA attribute_table(keep = stock_code end_date lock_month is_structure);
	SET product.raw_data;
	end_date = adj_report_date;
RUN;

%attribute_to_event(alpha_table=&eventName._alpha, attribute_table=attribute_table, alpha_table_edit=&eventName._alpha);
%alpha_collect_by_group(eventName=&eventName., alpha_table=&eventName._alpha,
		alpha_var=alpha, is_group=1, group_var=lock_month, is_output = 1, file_name=&output_dir./result.xls, sheet_name=lock_month);

/*************** �������������ԣ� �¼��о���� */


/*************** Step6: ģ�黯�¼��о���ܣ���ʽʹ��) */
/** Step1: ׼�����Ա�����֮��������ʹ�� */
DATA attribute_table(keep = stock_code end_date lock_month is_structure);
	SET product.raw_data;
	end_date = adj_report_date;
RUN;

%MACRO event_module(event_name, event_table, group_var, alpha_var=accum_alpha, start_win=-60, end_win=60, buy_win=0, win_ahead=-10);
	%gen_no_overlapped_win(eventName=&event_name., event_table=&event_table., stock_hqinfo_table=stock_hqinfo_subset, 
		bm_hqinfo_table=index_hqinfo_subset, start_win=&start_win., end_win=&end_win.);
	%mark_event_win(event_win_table=&event_name._hq, stock_info_table=stock_info_table, market_table = market_table, 
		output_table = &event_name._hq);	
	%mark_stat(event_win_table = &event_name._hq, output_table = &event_name._hq_stat);
	%cal_win_ret(eventName=hold, event_hq_table=product.hold_hq, buy_win=&buy_win.,  
		filter_invalid_after=1, filter_invalid_before=1, 
		filter_limit_after = 1, filter_limit_before = 1);
	/** �¼�ǰ���� */
	/*%append_ahead_effect(alpha_table=&eventName._alpha, win_ahead=&win_ahead., alpha_table_edit=&eventName._alpha);*/
	%attribute_to_event(alpha_table=&eventName._alpha, attribute_table=attribute_table, alpha_table_edit=&eventName._alpha);
	%alpha_collect_by_group(eventName=&eventName., alpha_table=&eventName._alpha,
		alpha_var=&alpha_var., is_group=1, group_var=&group_var., is_output = 1, file_name=&output_dir./result.xls, sheet_name=&group_var.);
	/** ����ͬʱ�䴰���µ������г�������Ҳһ����� */
	%output_to_excel(excel_path=&output_dir./result.xls, input_table=&event_name._hq_stat, sheet_name = win_stat);
	
%MEND event_module;

%event_module(event_name = hold, event_table=event_table2, group_var = is_structure, alpha_var = accum_alpha);


/************************************** ��������paper��� *****************************/
/** 1: ���ղ����������� */
/** ԭʼ�����У���һЩcover_num=0������senior_num!=0����senior_numҲ����0������ */
DATA attribute_table(keep = stock_code end_date cover_g);
	SET product.raw_data;
	end_date = adj_report_date;
	IF cover_num < 50 THEN cover_g = 1;
	ELSE IF cover_num >= 50 AND cover_num <100 THEN cover_g = 2;
	ELSE IF cover_num >= 100 AND cover_num < 150 THEN cover_g = 3;
	ELSE IF cover_num >= 150 AND cover_num < 300 THEN cover_g = 4;
	ELSE IF cover_num >= 300 AND cover_num < 500 THEN cover_g = 5;
	ELSE IF cover_num >= 500 AND cover_num < 1000 THEN cover_g = 6;
	ELSE IF cover_num >= 1000 AND cover_num < 1500 THEN cover_g = 7;
	ELSE IF cover_num >= 1500 AND cover_num < 5000 THEN cover_g = 8;
	ELSE cover_g = 9;
RUN;
%event_module(event_name = hold, event_table=event_table2, group_var = cover_g, alpha_var = accum_alpha);

/** 1: ���ղ����������� */
/** �����Ƿ��и߹ܲ��� */
DATA attribute_table(keep = stock_code end_date senior_g);
	SET product.raw_data;
	end_date = adj_report_date;
	IF cover_num = 0 AND senior_num = 0 THEN senior_g = 1;
	ELSE IF senior_num = 0 THEN senior_g = 2;
	ELSE senior_g = 3;
RUN;
%event_module(event_name = hold, event_table=event_table2, group_var = senior_g, alpha_var = accum_alpha);


