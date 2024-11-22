"""
Helper functions to run the project
List of disabled pylint errors:
C0301: Line too long (237/100) (line-too-long)
E0401: Unable to import '<library>' (import-error)
C0103: Constant name "<name>" doesn't conform to UPPER_CASE naming style (invalid-name)
W0703: Catching too general exception Exception (broad-except)
E1120: No value for argument 'src_row_count' in method call (no-value-for-parameter)
R0801: Similar lines in 2 files
I1101: Module 'cx_Oracle' has no 'connect' member, but source is unavailable.
Consider adding this module to extension-pkg-allow-list if you want to perform
analysis based on run-time introspection of living objects. (c-extension-no-member)
R0913: Too many arguments (6/5) (too-many-arguments)
"""
# pylint: disable=C0301
# pylint: disable=E0401
# pylint: disable=C0103
# pylint: disable=W0703
# pylint: disable=E1120
# pylint: disable=R0801
# pylint: disable=I1101
# pylint: disable=R0913

import os
import configparser
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import jaydebeapi
import cx_Oracle
from pyspark.sql import SparkSession

def set_environment_variables():
    """
    Set environment variables to run spark application
    """
    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2/lib/spark2"
    os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/ANACONDA/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/cloudera/parcels/ANACONDA/bin/python"
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--jars /opt/cloudera/parcels/JAVA_LIBS/kudu-spark2_2.11-1.2.0-cdh5.10.2.jar pyspark-shell"

def get_config(config_path):
    """
    Get config parameters from config.ini file
    """
    config = configparser.ConfigParser()
    config.read(f'{config_path}')
    return config

def update_impala_metadata(driver_cls, connection_str, jar_path, impala_query):
    """
    Update metadata using Impala
    """
    # Connect to Impala
    conn = jaydebeapi.connect(driver_cls, connection_str, jars=jar_path)
    # Create cursor for query execution
    impala_cursor = conn.cursor()
    # Execute Impala query
    impala_cursor.execute(impala_query)
    # Close connection
    conn.close()

def create_spark_session(appName):
    """
    Create spark_session method
    """
    spark = SparkSession.builder \
        .appName(appName) \
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .config("hive.exec.dynamic.partition", "true")\
        .config("spark.sql.sources.partitionOverwriteMode","dynamic")\
        .config("spark.driver.extraClassPath", "/opt/cloudera/parcels/JAVA_LIBS/ojdbc8.jar")\
        .config("spark.executor.extraClassPath", "/opt/cloudera/parcels/JAVA_LIBS/ojdbc8.jar")\
        .config("spark.kryoserializer.buffer.max", "1024") \
        .config("spark.executor.cores", 1) \
        .config("spark.driver.cores", 5) \
        .config("spark.executor.memoryOverhead", "7g") \
        .config("spark.driver.memoryOverhead", "7g") \
        .config("spark.executor.memory", "70g") \
        .config("spark.memory.offHeap.size", "3g") \
        .config("spark.driver.memory", "70g") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def dwh_connection_AP_RISK(login, password):
    """
    Inner-function for functions dwh_update and dwh_insert
    f'{login}/{password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=DBHDWKZ.KZ.PROD)(PORT=1521))(CONNECT_DATA=(UR=A)(SERVER=DEDICATED)(SERVICE_NAME=HDWKZ.KZ.PROD)))',
    """
    connection = cx_Oracle.connect(
        f'{login}/{password}@DBHDWKZ.KZ.PROD:1521/HDWKZ.KZ.PROD',
        encoding='UTF-8',
        nencoding='UTF-8'
    )
    return connection

def dwh_update(login, password):
    """
    Function to truncate a table via procedure
    """
    conn_ap_risk = dwh_connection_AP_RISK(login, password)
    cursor = cx_Oracle.Cursor(conn_ap_risk)

    sql_truncate = '''begin
    OWNER_EXT.TRUNCATE_TABLE_CONNECTION_SCORE_ACTUAL;
    end;'''
    cursor.execute(sql_truncate)

    conn_ap_risk.commit()

    cursor.close()
    conn_ap_risk.close()

def dwh_insert(data, login, password):
    """
    Function to insert data into table
    """
    conn_ap_risk = dwh_connection_AP_RISK(login, password)
    cursor = cx_Oracle.Cursor(conn_ap_risk)
    df = data.astype(str)
    rowss = df[['PHONE_NUMBER', 'ID_CUID', 'DATE_', 'SCORE', 'SK_DATE_SCORE', 'SCORE_GROUP', 'SK_DTIME_SCORE']].to_records(index=False).tolist()

    # cursor.prepare('''insert into ap_risk.t_zz_connection_score_history (PHONE_NUMBER, ID_CUID, DATE_, SCORE, SK_DATE_SCORE, SCORE_GROUP, SK_DTIME_SCORE) values (:1, :2, to_date(:3, 'dd.mm.yyyy'), :4, :5, :6, to_date(:7, 'dd.mm.yyyy HH24:MI:SS') )''')
    # cursor.executemany(None, rowss)

    cursor.prepare('''insert into OWNER_EXT.T_ZZ_CONNECTION_SCORE_ACTUAL (PHONE_NUMBER, ID_CUID, DATE_, SCORE, SK_DATE_SCORE, SCORE_GROUP, SK_DTIME_SCORE) values (:1, :2, to_date(:3, 'dd.mm.yyyy'), :4, :5, :6, to_date(:7, 'dd.mm.yyyy HH24:MI:SS') )''')
    cursor.executemany(None, rowss)

    conn_ap_risk.commit()
    cursor.close()
    conn_ap_risk.close()

def create_html_table(headers, data):
    """
    Function to prepare content of message to send
    """
    table = "<table border='1'>"

    # Заголовки
    table += "<thead><tr>"
    for header in headers:
        table += f"<th>{header}</th>"
    table += "</tr></thead>"

    # Данные
    table += "<tbody>"
    # for row in data:
    table += "<tr>"
    for cell in data:
        table += f"<td>{cell}</td>"
    table += "</tr>"
    table += "</tbody>"

    table += "</table>"
    return table

def send_email(subject, to_email, smtp_server, smtp_port, smtp_user, html_content):
    """
    Function to send email to specified users
    """
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = '; '.join(to_email)
    msg['Subject'] = subject

    # Добавляем HTML-содержимое
    msg.attach(MIMEText(html_content, 'html'))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.sendmail(smtp_user, to_email, msg.as_string())
            print(f"Email sent to {to_email}!")
    except Exception as e:
        print(f"Failed to send email to {to_email}. Error: {e}")
