/**����ת�Ƿ��Ԥ�⣬ֻ����걨���ݡ�**/


/********************** Step1: Ԥ��Ч�� **************/
/** ע�⣺
(1) ����ĩ�ܹɱ�(qmzgb)�ֶ�Ϊ��׼������share(ԭ���ɱ��ֶ�)��ʷ����361����¼��������ĩ�ܹɱ��ֶΡ�ԭ��δ֪.
(2) ֻѡ��acc_type=2(�»��׼��) & report_type="�ϲ�����"�ļ�¼
(3) 20061231��ʼ�����»��׼������й�˾�������ȶ�

**/
PROC SQL;
	CREATE TABLE stat1 AS
	SELECT report_period, count(1) AS nobs
	FROM event_db.zcfzb
	WHERE acc_type = "2" AND report_type="�ϲ�����"
	GROUP BY report_period;
QUIT;

/** Step1: ����Ԥ��ָ�� */
/* 	(1) �ܹɱ�
	(2) ÿ�ɾ��ʲ�
	(3) ÿ���ʱ�����
	(4) ÿ��δ��������
	(5) ÿ���������� = δ�������� + ӯ�๫��
	(6) ÿ���ʱ�����+��������
	(7) �ɼ�(ƽ��һ���¹���)
	(8) ����ʱ��
**/

/* (1) ȡ�����ȲƱ����ݣ���2007�꿪ʼ */
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
	WHERE acc_type = "2" AND report_type="�ϲ�����" AND report_period-floor(report_period/10000)*10000=930
		AND not missing(qmzgb)
	ORDER BY report_period, stock_code;
QUIT;

/** ����ת��Ϣ���� */
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
	(SELECT * FROM fh WHERE fh_year-floor(fh_year/10000)*10000=1231 AND not missing(sz_fz)) B  /* ֻ�����걨 */
	ON A.stock_code = B.stock_code AND floor(B.fh_year/10000) = floor(A.report_period/10000)
	WHERE floor(report_period/10000) >= 2007 AND floor(report_period/10000) <= 2014
	ORDER BY A.report_period, A.stock_code;
QUIT;

/*** (2) ��������Ϣ�����ӣ����10��ƽ���ɼ� */
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

/*** (3) ����ʱ�� */
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


/** ����ÿ�����ת����(precision)��ÿ��ƽ����ת�����Ͳ�ȫ��(recall) */
/** ����ָ�꣺F-score = 2*precision*recall/(precision+recall) */

%LET factor = list_date;
%LET desc_str = descending;

PROC RANK DATA = fdata_fh OUT = fdata_fh_rank TIES = low &desc_str. GROUPS = 10;
	VAR &factor.;
	RANKS r_&factor.;
RUN;

/** ��ɢ�������÷������ʽ����������� */

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


/** ����� */
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
/** ת�ÿ�ѡ */
PROC TRANSPOSE DATA = fdata_fh_class_year OUT = fdata_fh_class_year;
	BY year;
	ID r_&factor.;
RUN;
PROC SORT DATA = fdata_fh_class_year(DROP=_LABEL_ RENAME=(_NAME_=variable));
	BY variable year;
RUN;


/** Step3-2 ��֤����Ԥ�����Ƿ�ȼ��ڸߵĳ�������? */
/** ��������Ϊ������
	(1)��ʵ��������ת���ڲ���Ϊ0-4�飬����ǰ��Ԥ��÷ִӸߵ��ͷ���
	(2)δ��������ת�飬�ڲ���Ϊ0-4�飬����ǰ��Ԥ��÷ִӸߵ��ͷ���(������Ϊ��Ԥ�������գ����ͳһ�趨Ϊ11��1��)
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
/** ����filter�ֲ� */
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

/** ͳ��ָ�� */
%event_cal_stat(rtn_table=gsz_rtn, rtn_var=accum_alpha_sm, output_table=alpha_stat, filter_set=(1,2,3,4,5), group_var=r_&factor.);
%event_addindex_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, buy_win=0, output_table=alpha_stat, group_var=r_&factor.);
/* ��λ����index */
%event_addindex_stat(stat_table=alpha_stat, rtn_var=pct50, buy_win=0, output_table=alpha_stat, group_var=r_&factor.);

%event_mdf_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, output_table=alpha_stat_n,
					group_var=r_&factor., win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120));


/** ���ֻ������ֵ��� */
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


