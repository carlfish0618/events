/**����תͳ�Ʒ�����**/

/*** ���Կ�ʼʱ��Ϊ��2006/1/1 ****/
/** �ⲿ���ݿ⣺
(1) event_db.fh: ԤԼ��¶��
(2) event_db.earning_actual: �������
(3) event_db.zcfzb �ʲ���ծ��
***/

%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\�޸İ汾; 

%LET input_dir = &product_dir.\input_data\����ת; 
%LET output_dir = &product_dir.\output_data\����ת;
LIBNAME product "&product_dir.\sasdata\����ת";

LIBNAME database "D:\Research\���ݿ�\ͨ��";
LIBNAME event_db "D:\Research\���ݿ�\�¼����ݿ�";

%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\�¼��о�_ͨ�ú���_�汾2.sas";


%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\Ȩ��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ϲ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\������Ч��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\���Ӽ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ч����_ͨ�ú���.sas";

options validvarname=any; /* ֧�����ı����� */


/*** step0: �����¼��о�_�����ļ� **/
%LET env_start_date = 30jun2005;
%LET index_code = 000300;
%INCLUDE "&utils_dir.\�¼��о�_�����ļ�_���ذ汾_�ʺϰ汾2.sas";


/******** ����ת�¼��������� ********/
/** 1-�ֺ��
	(1) stock_code
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
	SET event_db.fh;
	sz_fz = coalesce(sg_fz,0)+coalesce(zz_fz,0);
	year = floor(yaggr/10000);
	period = floor((fh_year-floor(fh_year/10000)*10000)/100);
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
/** ��0.005�ֵ� */
PROC SQL;
	CREATE TABLE fh_px_class AS
	SELECT year, sum(px_rate>0) AS  nobs_sz,
		sum(floor(px_rate/0.005)>=2) AS nobs_gpx, /* �����֣�����Ϊ�ֺ�������1% */
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



/*** Step2��Ԥ�������������ֲ�*/
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

/*** Step3: Ԥ��ʱ��ֲ� */
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

