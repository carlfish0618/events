/*** ��Ȩ���� **/
/** ����������ͳ�Ʒ��� */
/*** ���Կ�ʼʱ��Ϊ��2006/1/1 ****/
/** �ⲿ���ݿ⣺
(1) event_db.ashareincdescription: ��Ȩ����
***/


%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\�޸İ汾; 

%LET input_dir = &product_dir.\input_data\��Ȩ����; 
%LET output_dir = &product_dir.\output_data\��Ȩ����;
LIBNAME product "&product_dir.\sasdata\��Ȩ����";

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

/******************* Step0: �����¼��о�_�����ļ� **/
%LET env_start_date = 30jun2005;
%LET index_code = 000300;
%INCLUDE "&utils_dir.\�¼��о�_�����ļ�_���ذ汾_�ʺϰ汾2.sas";


/**** Step1: �����ֲ����� **/
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

/**** Step2: �����ؼ�ʱ���ķֲ� **/
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

/** Step3: �ؼ�ʱ���֮��ļ�� */
%LET dif_var = dif2;
DATA tt;
	SET gqjl;
	dif1 = gddhr - yaggr;
	dif2 = ssggr - gddhr;
	dif3 = qsr - ssggr;
	mark = 1;
RUN;
%cal_dist(input_table=tt, by_var=mark, cal_var=&dif_var., out_table=stat, pctlpts=100 75 50 25 0);


/** Step4: ���Ȱ��� */
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
