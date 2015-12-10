%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\�޸İ汾; 

%LET input_dir = &product_dir.\input_data\����ת; 
%LET output_dir = &product_dir.\output_data\����ת;
LIBNAME product "&product_dir.\sasdata\����ת";

LIBNAME database "D:\Research\���ݿ�\ͨ��";

%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\Ȩ��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ϲ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\�¼��о�_ͨ�ú���_�汾2.sas";
%INCLUDE "&utils_dir.\������Ч��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\���Ӽ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ч����_ͨ�ú���.sas";

options validvarname=any; /* ֧�����ı����� */

/*** step0: �����¼��о�_�����ļ� **/
%LET env_start_date = 30jun2005;
%LET index_code = 000905;
%INCLUDE "&utils_dir.\�¼��о�_�����ļ�_���ذ汾_�ʺϰ汾2.sas";

/******** ����ת�¼��������� ********/
/** 1-�ֺ��
	(1) stock_id
	(2) process: ʵʩ
	(3) ggrq: ��������(numeric)
	(4) fh_year���ֺ����
	(5) iffz: 
	(6) sg_fm/sg_fz: �͹ɷ�ĸ/����
	(7) zz_fm/zz_fz: ת����ĸ/����
	(8) px_fm/px_fzsq/px_fzsh�����ַ�ĸ/���ַ���˰ǰ/���ַ���˰��
	(9) gqdjr/cqcxr/yaggr/gddhggr/fhssggr: ��Ȩ�Ǽ���/��Ȩ��Ϣ��/Ԥ��������/�ɶ���ṫ����/�ֺ�ʵʩ������
	(10) year��Ԥ�����������ڵ����
***/

/**** Step1: ����ͳ�Ʒ��� */
/** Step1-1: ��ת����(�͹�+ת��) */
DATA fh;
	SET product.fh;
	sz_fz = coalesce(sg_fz,0)+coalesce(zz_fz,0);
RUN;

PROC SQL;
	CREATE TABLE fh_sz_class AS
	SELECT year, sum(sz_fz>0) AS  nobs_sz,
		sum(floor(sz_fz)>=5) AS nobs_gsz1, /* ����ת1������Ϊ��ת�ɱ���>=5 */
		sum(floor(sz_fz)>=10) AS nobs_gsz2, /* ����ת2�� ��ת�ɱ���>=10 */
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


/** Step1-2: �ֺ�������(˰��������/Ԥ�������յ�ǰ���̼۸�) */
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
/** ��0.005�ֵ� */
PROC SQL;
	CREATE TABLE fh_px_class AS
	SELECT year, sum(px_rate>0) AS  nobs_sz,
		sum(floor(px_rate/0.005)>=2) AS nobs_gpx, /* �����֣�����Ϊ��ת�ɱ���>=5 */
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

/** Step1-3: ��Ϊ3��ͳ�ƣ�
	(1) ����ת1��������
	(2) ����ת1, ������
	(3) ����ת��������
	(4) ����ת��������
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


/**************************** Step2: �¼��о� */
/** 10��5������ */
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
%event_cal_stat(rtn_table=gsz_rtn, rtn_var=accum_alpha_sm, output_table=alpha_stat, filter_set=(1,2,3,4,5), group_var=year);
%event_addindex_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, buy_win=0, output_table=alpha_stat, group_var=year);
/* ��λ����index */
%event_addindex_stat(stat_table=alpha_stat, rtn_var=pct50, buy_win=0, output_table=alpha_stat, group_var=year);

%event_mdf_stat(stat_table=alpha_stat, rtn_var=accum_alpha_sm, output_table=alpha_stat_n,
					group_var=year, win_set=(-120,-60,-40,-20,-10,-5,0,5,10,20,40,60,120));


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

/********************* Step3: Ԥ��Ч�� **************/
/** ע�⣺
(1) ����ĩ�ܹɱ�(qmzgb)�ֶ�Ϊ��׼������share(ԭ���ɱ��ֶ�)��ʷ����361����¼��������ĩ�ܹɱ��ֶΡ�ԭ��δ֪.
(2) ֻѡ��acc_type=2(�»��׼��) & report_type="�ϲ�����"�ļ�¼
(3) 20061231��ʼ�����»��׼������й�˾�������ȶ�

**/
PROC SQL;
	CREATE TABLE stat1 AS
	SELECT report_period, count(1) AS nobs
	FROM product.zcfzb
	WHERE acc_type = "2" AND report_type="�ϲ�����"
	GROUP BY report_period;
QUIT;

/** Step3-1Ԥ��ָ������(��������������Ϊ׼) */
/* 	(1) �ܹɱ�
	(2) ÿ�ɾ��ʲ�
	(3) ÿ���ʱ�����
	(4) ÿ��δ��������
	(5) ÿ���������� = δ�������� + ӯ�๫��
	(6) ÿ���ʱ�����+��������
	(7) �ɼ�(ƽ��һ���¹���)
**/

/* ȡ�����ȲƱ����ݣ���2007�꿪ʼ */
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
		WHEN coalesce(B.sz_fz,0)>= 5 THEN 1
		ELSE 0
	END AS is_gsz,
	floor(report_period/10000) AS year
	FROM fdata A LEFT JOIN 
	(SELECT * FROM fh WHERE fh_year-floor(fh_year/10000)*10000=1231 AND not missing(sz_fz)) B  /* ֻ�����걨 */
	ON A.stock_code = B.stock_id AND floor(B.fh_year/10000) = floor(A.report_period/10000)
	WHERE floor(report_period/10000) >= 2007 AND floor(report_period/10000) <= 2014
	ORDER BY A.report_period, A.stock_code;
QUIT;

/** ����ÿ�����ת����(precision)��ÿ��ƽ����ת�����Ͳ�ȫ��(recall) */
/** ����ָ�꣺F-score = 2*precision*recall/(precision+recall) */

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


		


	
