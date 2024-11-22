"""
Module: connection_score_generator
List of disabled pylint errors:
C0301: Line too long (237/100) (line-too-long)
E0401: Unable to import '<library>' (import-error)
C0103: Constant name "<name>" doesn't conform to UPPER_CASE naming style (invalid-name)
W0703: Catching too general exception Exception (broad-except)
E1120: No value for argument 'src_row_count' in method call (no-value-for-parameter)
R0801: Similar lines in 2 files
"""
# pylint: disable=C0301
# pylint: disable=E0401
# pylint: disable=C0103
# pylint: disable=W0703
# pylint: disable=E1120
# pylint: disable=R0801

import time
import datetime
import subprocess
from datetime import datetime
import pickle
import numpy as np
from pyspark_job_control.pyspark_job_control_logs import JobControlLogs
from pyspark.sql.functions import col
from core_functions import (
    get_config,
    set_environment_variables,
    update_impala_metadata,
    create_spark_session,
    dwh_update,
    dwh_insert,
    create_html_table,
    send_email
)

# region - local variables.
PROJECT_PATH = '/opt/apps/kz.bdp.risk/PythonJobs/models/connection_score_generator'
CONFIG_PATH = f'{PROJECT_PATH}/config.ini'
config = get_config(CONFIG_PATH)

appName="init_connection_score_generator"
src_db_nm="bdp_feature_offline_stg"
src_table_nm="stg_cs_daily_call_feautures"
trg_sys_name="bdp_score_stg"
trg_tab_name="daily_connection_score23_hdfs"
dwh_table_name="OWNER_EXT.T_ZZ_CONNECTION_SCORE_ACTUAL"
pickle_file_path = f"{PROJECT_PATH}/model_connect_score_20_final.pkl"

load_method =   "INITIAL"
start_status =  "STARTED"
finish_status = "FINISHED"
error_status =  "ERROR"
msg = ""

cx_oracle_url =             config['oracle_hadoopetl']['cx_oracle_url']
job_control_logs =          config['oracle_hadoopetl']['job_control_logs']
job_control_tables_list =   config['oracle_hadoopetl']['job_control_tables_list']
ora_procedure_insert =      config['oracle_hadoopetl']['ora_procedure_insert']
ora_procedure_update =      config['oracle_hadoopetl']['ora_procedure_update']

dm_bdp_etl_user =   config['oracle']['dm_bdp_etl_user']
dm_bdp_etl_pass =   config['oracle']['dm_bdp_etl_pass']

impala_driver =     config['impala']['driver_class']
impala_conn_str =   config['impala']['connection_string']
impala_jar_path =   config['impala']['jar_path']

impala_refresh_query_1 = f"alter table {trg_sys_name}.{trg_tab_name} recover partitions"
impala_refresh_query_2 = f"refresh {trg_sys_name}.{trg_tab_name}"

num_bins = 10

SMTP_SERVER = 'smtp-int.itc.homecredit.kz'
SMTP_PORT = 25
SMTP_USER = 'Zhalgas.Zhienbekov@homecredit.kz'
SUBJECT = 'Hadoop Alert | Connection Score'
RECEIVERS = ['e-akmal.atkhamov@homecredit.kz',
            'sergey.korniyenko@homecredit.kz',
            'issa.osser@homecredit.kz',
            'denis.kan@homecredit.kz',
            'matej.pjontek1@homecredit.kz',
            'Zhalgas.Zhienbekov@homecredit.kz']

max_src_date = datetime.strptime(datetime.now().strftime('%d.%m.%Y'), '%d.%m.%Y')
os_user = subprocess.check_output(['whoami'], universal_newlines=True).strip()
job_id = JobControlLogs.get_job_id(
    cx_oracle_url=cx_oracle_url,
    job_control_tables_list=job_control_tables_list,
    tgt_db_nm=trg_sys_name,
    tgt_table_nm=trg_tab_name,
    src_db_nm=src_db_nm,
    src_table_nm=src_table_nm
)
# endregion - local variables

# Create job_control object
job_control_obj = JobControlLogs(
    cx_oracle_url=cx_oracle_url,
    job_control_logs=job_control_logs,
    job_control_tables_list=job_control_tables_list,
    ora_procedure_insert=ora_procedure_insert,
    ora_procedure_update=ora_procedure_update,
    os_user=os_user,
    job_id=job_id
)

# Check if job is active
job_is_active = int(
    job_control_obj.get_is_active(
        tgt_db_nm=trg_sys_name,
        tgt_table_nm=trg_tab_name,
        src_db_nm=src_db_nm,
        src_table_nm=src_table_nm
    )
)

# Get next_id for logging table
next_id = job_control_obj.get_next_id_cx_oracle()

# Start execution of job
if job_is_active > 0:
    try:
        # generate job_start_time for email notification
        job_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job_control_obj.call_insert_job_control(
            p_next_id=next_id,
            p_appname=f"{appName}",
            p_job_status=start_status,
            p_src_db=src_db_nm,
            p_src_table=src_table_nm,
            p_tgt_db=trg_sys_name,
            p_tgt_table=trg_tab_name,
            p_load_method=load_method,
            p_job_id=job_id
        )
        # Set environment variables
        set_environment_variables()

        # Create spark session
        spark = create_spark_session(appName)

        df = spark.read.parquet(f"/user/hive/warehouse/{src_db_nm}.db/{src_table_nm}")
        df = df.withColumn("phone_number", col("phone_number").cast("string"))
        print(df.show(10))
        print("DF COUNT:", df.count())
        pd_data = df.toPandas()
        print("PD_DATA COLUMNS:", pd_data.columns)

        # DATA PREPARATION
        pd_data[['phone_number', 'rate_connect_mor', 'rate_connect_after',
                'rate_connect_even', 'rate_connect_monday', 'rate_connect_tuesday',
                'rate_connect_wednesday', 'rate_connect_thursday',
                'rate_connect_friday', 'rate_connect_saturday', 'rate_connect_sunday',
                'cnt_in_calls_6m', 'past_days_last_succ_call', 'past_days_last_call',
                'cnt_success_sms_6m']] = pd_data[['phone_number', 'rate_connect_mor', 'rate_connect_after',
                'rate_connect_even', 'rate_connect_monday', 'rate_connect_tuesday',
                'rate_connect_wednesday', 'rate_connect_thursday',
                'rate_connect_friday', 'rate_connect_saturday', 'rate_connect_sunday',
                'cnt_in_calls_6m', 'past_days_last_succ_call', 'past_days_last_call',
                'cnt_success_sms_6m']].fillna(0)
        print(pd_data.shape)

        pd_data[['pif_phone_type_code', 'last_interaction_type_code']] = pd_data[['pif_phone_type_code', 'last_interaction_type_code']].astype('category')
        data = pd_data.copy()
        pd_data = pd_data[(pd_data['phone_number'].str.len() == 11) | (pd_data['phone_number'].str.len() == 10)]
        pd_data['cuid'] = pd_data['cuid'].fillna(-1)

        # MODEL PREDICTION
        with open(pickle_file_path, "rb") as file:
            model_cs_2 = pickle.load(file)

        # pd_data.head()
        print("MODEL FEATURE NAME:")
        model_cs_2.feature_name()
        data_week = pd_data[['past_days_last_call',
                            'cnt_success_sms_6m',
                            'rate_connect_monday',
                            'rate_connect_friday',
                            'rate_connect_tuesday',
                            'rate_connect_thursday',
                            'cnt_in_calls_6m',
                            'rate_connect_wednesday',
                            'rate_connect_sunday',
                            'rate_connect_saturday',
                            'pif_phone_type_code']]
        week_num = datetime.now().weekday() + 1
        data_scores = pd_data[['phone_number','cuid']]
        data_week.loc[:, 'weekday'] = week_num
        print("SHAPE 1:", data_week.shape)
        data_week['weekday'] = data_week['weekday'].astype('category')
        print("SHAPE 2:", data_week.shape)
        week_score = model_cs_2.predict(data_week)
        data_scores['SCORE'] = week_score
        data_scores['SK_DTIME_SCORE'] = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        data_scores['SK_DATE_SCORE'] = datetime.now().strftime("%Y%m%d")
        data_scores['DATE_'] = datetime.now().strftime("%d.%m.%Y")
        data_scores = data_scores.rename(columns = lambda x:x.upper())
        data_scores['PHONE_NUMBER'] = data_scores['PHONE_NUMBER'].astype(str)
        data_scores['CUID'] = data_scores['CUID'].fillna(-1)
        data_scores['CUID'] = data_scores['CUID'].astype(int)
        data_test = data_scores[['PHONE_NUMBER','CUID','DATE_','SCORE','SK_DATE_SCORE','SK_DTIME_SCORE']]
        print("DATA_TEST:", data_test)
        data_test = data_test.sort_values(by='SCORE', ascending=False).reset_index(drop=True)
        num_bins = 10
        bin_edges = np.linspace(0, len(data_test), num_bins + 1).astype(int)
        data_test['SCORE_GROUP'] = 0
        for i in range(num_bins):
            data_test.loc[bin_edges[i]:bin_edges[i+1]-1, 'SCORE_GROUP'] = i + 1
        data_test = data_test.rename(columns = {'CUID':'ID_CUID'})
        data_test = data_test[['PHONE_NUMBER','ID_CUID','DATE_','SCORE','SK_DATE_SCORE','SCORE_GROUP','SK_DTIME_SCORE']]
        data_test['SK_DATE_SCORE'] = data_test['SK_DATE_SCORE'].astype(int)
        print("DATA_TEST data_types:", data_test.dtypes)

        # EXPORT DATA TO HADOOP SERVER.
        df_to_hdfs = spark.createDataFrame(data_test)
        quantity = df_to_hdfs.count()
        part_date = datetime.now().strftime("%Y%m%d")
        df_to_hdfs.write.format("parquet").mode('overwrite')\
            .save(f"/user/hive/warehouse/{trg_sys_name}.db/{trg_tab_name}/part_date={part_date}")

        # UPDATE IMPALA METADATA
        update_impala_metadata(impala_driver, impala_conn_str, impala_jar_path, impala_refresh_query_1)
        update_impala_metadata(impala_driver, impala_conn_str, impala_jar_path, impala_refresh_query_2)

        # EXPORT DATA TO DWH
        dwh_update(login=dm_bdp_etl_user, password=dm_bdp_etl_pass)
        start_time = time.time()

        for i in range(11):
            data_part = data_test.iloc[i*2000000:(i+1)*2000000]
            dwh_insert(data=data_part, login=dm_bdp_etl_user, password=dm_bdp_etl_pass)
            print(f'Inserted data from rows {i*2100000} to {(i+1)*2100000}')
            print('Loading...')
        print('Success, table is ready to read!')

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Elapsed Time: {elapsed_time} seconds")

        # generate job_end_time for email notification
        job_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Log the successful end of the job
        job_control_obj.call_update_job_control(
            job_control_id=next_id,
            job_status=finish_status,
            src_row_count=quantity,
            tgt_row_count=quantity,
            p_max_src_dt=max_src_date
        )

        column_names = ['start_time', 'end_time', 'job_name', 'target_table', 'job_status', 'row_count', 'part_date']
        column_values = [job_start_time, job_end_time, appName, dwh_table_name, finish_status, quantity, part_date]

        html_content = create_html_table(column_names, column_values)
        send_email(SUBJECT, RECEIVERS, SMTP_SERVER, SMTP_PORT, SMTP_USER, html_content)
    except Exception as exception_message:
        if not msg:
            msg = Exception(f"An error occurred: {exception_message}")
        # Log the error of the job
        job_control_obj.call_update_job_control(
            job_control_id=next_id,
            job_status=error_status,
            src_row_count='0',
            tgt_row_count='0',
            p_max_src_dt=max_src_date,
            p_err_msg=str(msg)[:500]
        )
        print(str(msg))
        # Send notification to teams in case of error
        job_control_obj.send_notification_to_teams(str(msg), f"Info! {appName}")
        # raise and exception to make the job in Airflow to become failed.
        raise Exception(f"Forced exception. Original error message: {exception_message}") from exception_message
else:
    job_control_obj.send_notification_to_teams("Table has is_active=0 status. Please, change is_active to 1", f"Info! {appName}")

spark.stop()
