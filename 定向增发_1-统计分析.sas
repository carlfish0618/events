/*** �ɶ��߹ܶ����г������� **/

/** �ⲿ�ļ�(����wind)��
	����������������_����20151208.xlsx
	��������Ԥ��_����20151208.xlsx
	����ʵʩ_����20151208.xlsx
***/

/** �ļ��ֶΣ� 
(1) ��������Ԥ����
����
����
Ԥ��������
��������
���з�ʽ
�����۸�
Ԥ��������
���¼�
Ԥ�������(%)
����
���۹���
��������(���)
Ԥ��ļ���ʽ�(��Ԫ)
��������Ŀ��
��������˵��
���ж���
���ж���˵��
�Ϲ���ʽ
��������
�ɶ���ṫ����
����ίͨ��������
֤�����׼������
Ԥ�⾻����(��Ԫ)
��������
�������
Ԥ�����
֤�����ҵ(2012��)
֤�����ҵ
Wind��ҵ
֤ȯ����
(2) ����������������
����
����
����������
���ۻ�׼��
�����۸�
�Է��м��ǵ���(��Ȩ)
Ԥ��������
��������(���)
Ԥ��ļ���ʽ�(��Ԫ)
ʵ��ļ���ܶ�(��Ԫ)
���з���(��Ԫ)
ʵ��ļ�ʾ���(��Ԫ)
���з���(%)
��������Ŀ��
��������Ŀ��˵��
���ж���
��������
�����ۼ���(%)
��ɶ��Ƿ�����Ϲ�
��ɶ��Ϲ�����(%)
��ɶ��Ϲ����(��Ԫ)
��ɶ��Ϲ���ʽ
�����۸��ƶ�����
�����׼������
ʵʩ����Ի�׼�۸����(%)
Ԥ������Ի�׼�۸����(%)
������ʽ
��������
�������
Ԥ��������
�ɶ���ṫ����
����ίͨ��������
֤����׼������
��������
��Ȩ�Ǽ���
�����ɷݱ䶯��
���۹ɷݽ����
��ҵ����
֤�����ҵ(2012��)
֤�����ҵ
Wind��ҵ
��������
(3) ����ʵʩ
����
����
���д���
����������
��������
���з�ʽ
���м۸�
���������̼�
�������̼�
�����̼�
�Է��м��ǵ���(% ��Ȩ)
��������(���)
Ԥ��ļ���ʽ�(��Ԫ)
ʵ��ļ���ܶ�(��Ԫ)
���з���(��Ԫ)
ʵ��ļ�ʾ���(��Ԫ)
���ж���
�Ϲ���ʽ
��������
������ʽ
��������
�������
���۹ɽ��ʱ��
֤�����ҵ(2012��)
֤�����ҵ
Wind��ҵ
֤ȯ����	
***/



%LET product_dir = D:\Research\GIT-BACKUP\events;
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\�޸İ汾; 

%LET input_dir = &product_dir.\input_data\������; 
%LET output_dir = &product_dir.\output_data\������;
LIBNAME product "&product_dir.\sasdata\������";

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

/**** Step1: ���ⲿ�������� **/
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2005.xlsx, output_table=product.d2005, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2006.xlsx, output_table=product.d2006, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2007.xlsx, output_table=product.d2007, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2008.xlsx, output_table=product.d2008, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2009.xlsx, output_table=product.d2009, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2010.xlsx, output_table=product.d2010, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2011.xlsx, output_table=product.d2011, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2012.xlsx, output_table=product.d2012, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2013.xlsx, output_table=product.d2013, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2014.xlsx, output_table=product.d2014, sheet_name = Wind��Ѷ$);
%read_from_excel(excel_path=&input_dir.\��Ҫ�ɶ������г�����_2015.xlsx, output_table=product.d2015, sheet_name = Wind��Ѷ$);

/*** Step2���������ֶν��д��� */
%MACRO modify_zjc_data(input_data, output_data);
	DATA &output_data.;
		SET &input_data.;
		IF trim(left(����)) = "������Դ��Wind��Ѷ" THEN delete;
		IF missing(����) THEN delete;
		LENGTH stock_code $6.;
		LENGTH stock_name $20.;
		LENGTH holder_name $100.;
		LENGTH holder_type $10.;
		LENGTH relation $20.;
		LENGTH direction $10.;
		stock_code = substr(����,1,6);
		stock_name = trim(left(����));
		pub_date = ��������;
		start_change_date = �䶯��ʼ����;
		start_end_date = �䶯��ֹ����;
		holder_name = �ɶ�����;
		holder_type = �ɶ�����;
		relation = �ɶ���߹ܹ�ϵ;
		direction = ����;
		shares = '�䶯����(���)'n;
		big_shares = 'ͨ�����ڽ���ϵͳ�ı䶯����(���)'n;
		free_pct = '�䶯����ռ��ͨ�ɱ�(%)'n;
		total_pct = '�䶯����ռ�ܹɱ�����(%)'n;
		after_free_shares = '�䶯�������ͨ������(���)'n;
		after_free_pct = '�䶯����ͨ������ռ��ͨ�ɱ�(%)'n;
		after_total_shares = '�䶯��ֹ�����(���)'n;
		after_total_pct = '�䶯��ֹɱ���(%)'n;
		trade_avg_price = ����ƽ����;
		avg_price = �䶯�ڼ��Ʊ����;
		value = '�䶯���ֲο���ֵ(��Ԫ)'n;
		is_lock = �Ƿ�Ϊ���ֽ���ɷ�;
		FORMAT pub_date start_change_date start_end_date yymmdd10.;
		DROP ����
		����
		��������
		�䶯��ʼ����
		�䶯��ֹ����
		�ɶ�����
		�ɶ�����
		�����߹�
		�ɶ���߹ܹ�ϵ
		����
		'�䶯����(���)'n
		'ͨ�����ڽ���ϵͳ�ı䶯����(���)'n
		'�䶯����ռ��ͨ�ɱ�(%)'n
		'�䶯����ռ�ܹɱ�����(%)'n
		'�䶯�������ͨ������(���)'n
		'�䶯����ͨ������ռ��ͨ�ɱ�(%)'n
		'�䶯��ֹ�����(���)'n
		'�䶯��ֹɱ���(%)'n
		����ƽ����
		�䶯�ڼ��Ʊ����
		'�䶯���ֲο���ֵ(��Ԫ)'n
		����
		�Ƿ�Ϊ���ֽ���ɷ�
		˵��
		֤�����ҵ
		Wind��ҵ;
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


/**** Step1: �����ֲ����� **/
DATA zjc;
	SET zjc;
	year = year(pub_date);
	month = month(pub_date);
	IF missing(pub_date) THEN delete;
RUN;



/**���ڹ�Ʊ����*/
PROC SQL;
	CREATE TABLE stat AS
	SELECT year, count(distinct stock_code) AS nobs,
		count(distinct input(stock_code,8.)*(holder_type="�߹�"))-1 AS nobs1,
		count(distinct input(stock_code,8.)*(holder_type="��˾"))-1 AS nobs2,
		count(distinct input(stock_code,8.)*(holder_type="����"))-1 AS nobs3
	FROM zjc
	WHERE direction="����" AND not missing(big_shares)
	GROUP BY year;
QUIT;



/*** �����漰�Ĵ��� */
PROC SQL;
	CREATE TABLE zjc_nodup AS
	SELECT stock_code, pub_date, direction, year, month, count(1) AS nobs
	FROM zjc
	WHERE holder_type = "�߹�" AND not missing(big_shares)
	GROUP BY stock_code, pub_date, direction, year, month;
QUIT;
PROc SQL;
	CREATE TABLE stat AS
	SELECT year, 
		sum(direction="����") AS nzc,
		sum(direction="����") AS njc
	FROM zjc_nodup
	GROUP BY year;
QUIT;
data tt;
	set zjc;
	if holder_type = "�߹�" AND missing(relation);
run;

