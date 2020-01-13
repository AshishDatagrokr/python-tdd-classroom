import mysql.connector
from mysql.connector import Error
from pyspark.sql.functions import col, lit
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import csv
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import col, lit

from pyspark.sql.functions import (col, concat, concat_ws, length, lit,
                                   substring, substring_index, to_timestamp,
                                   when, collect_list, count, explode, size)

from pyspark import SparkContext

import source_extract
import validation_custodial
from validation_business import val_exe_tbl
from notification import mail_notification, notification_alert
from validation_custodial import process_cloudsql, process_bigquery, process_gcs
from source_extract import write_results, load_table_cnt, load_table, load_data_from_cloudsql
import notification
#sc = SparkContext("local", "data validation")
sc = SparkContext.getOrCreate()
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)



spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('data validation') \
  .getOrCreate()
  
# load configuration File from Gcs.
conf_file_path = "/root/conf/datavalidation.conf"
conf_file_path_gcs = "gs://datagrokr/validation_framework/config/data_validation.conf "
subprocess.check_call(
            'gsutil cp {0}'
            ' {1}'.format(conf_file_path_gcs, conf_file_path).split())
file = open(conf_file_path,"r")
dic = dict()
for r in file.read().split('\n'):
    dic[r.split('=')[0]]=r.split('=')[1]


PROJECT = dic['project']
TABLE_NAME = dic['table_name']
DATASET_ID = dic['dataset_id']
output_table = dic['output_table']
output_dir = dic['output_dir']
output_agg_table = dic['output_agg_table']
obj_log_table = dic['obj_log_table']
user=dic['user']
pswd=dic['pswd']
hostip=dic['hostip']
database=dic['database']
execution_stat_table=dic['execution_stat_table']
host =  hostip
client = bigquery.Client(PROJECT)

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector. This assumes the Cloud Storage connector for
# Hadoop is configured.

bucket = spark.sparkContext._jsc.hadoopConfiguration().get('fs.gs.system.bucket')

spark.conf.set('temporaryGcsBucket', bucket)

def get_connection():    
    """ Connect to MySQL database """
    conn = None
    try:
        conn = mysql.connector.connect(host=hostip,
                                       database=database,
                                       user=user,
                                       password=pswd)
        if conn.is_connected():
            print('Connected to MySQL database')
 
    except Error as e:
        print(e)
    return conn

def calculate_validation_date(object_id, Time_Interval, Validation_Period, Validation_Frequency, job_id, Run_ID, client):

    # for calculating num of mins in a year, multiplied it by 365.25
    period_to_mins = {'full table': 15778800, 'daily': 1440, 'monthly': 43830, 'yearly': 15778800}  
    
    Last_Update_Date = datetime.utcnow()
    

    if Time_Interval == None:
        duration_min = int(period_to_mins[Validation_Period])
    else:
        duration_min = int(Time_Interval)
        

    query =f"""SELECT Prev_Val_St_Dt, Prev_Val_End_Dt, Curr_Val_St_Dt, Curr_Val_End_Dt ,Next_Val_St_Dt, Next_Val_End_Dt, Val_Status, Run_Id
                FROM `{DATASET_ID}.{execution_stat_table}` where Object_ID = {object_id} 
                and Last_Upd_Dt = (SELECT MAX(Last_Upd_Dt) from `{DATASET_ID}.{execution_stat_table}` where Object_ID = {object_id})"""

    #cursor.execute(query)
    df = client.query(query).to_dataframe()
    #df = pd.DataFrame(cursor.fetchall(), columns = ['Prev_Val_St_Dt','Prev_Val_End_Dt', 'Curr_Val_St_Dt', 'Curr_Val_End_Dt', 'Next_Val_St_Dt', 'Next_Val_End_Dt', 'Val_Status'])
    #print(query)
    print(df)
    inst_qry = f"""INSERT INTO `{DATASET_ID}.{execution_stat_table}` (Object_ID, Validation_Period, Validation_frequency, Run_Type, Prev_Val_St_Dt, Prev_Val_End_Dt, Curr_Val_St_Dt, Curr_Val_End_Dt, Next_Val_St_Dt, Next_Val_End_Dt, Val_Status, Last_Upd_Dt, Run_Id, Job_Id) VALUES
                    ({{0}}, '{{8}}', '{{9}}', 'Automatic', '{{1}}', '{{2}}', '{{3}}', '{{4}}','{{5}}', '{{6}}', 'Executing', '{{7}}', {{10}}, '{{11}}')"""

    if df.empty == False:
        print(duration_min)
        if df['Val_Status'][0] == 'Success':
            if Validation_Period == 'full table':
                Prev_Val_St_Dt  =  df['Curr_Val_St_Dt'][0]  
                Prev_Val_End_Dt =  df['Curr_Val_End_Dt'][0] 
                Val_St_Dt  =  datetime(1900, 1, 1, 0, 0, 0 )
                Val_End_Dt =  datetime.utcnow()
                Next_Val_St_Dt  =  Val_St_Dt
                Next_Val_End_Dt =  datetime.utcnow()+timedelta(days=1)
                print("In Success full table")
            else:
                Prev_Val_St_Dt  =  df['Next_Val_St_Dt'][0]+timedelta(minutes=-duration_min)
                Prev_Val_End_Dt =  df['Next_Val_End_Dt'][0]+timedelta(minutes=-duration_min)
                Val_St_Dt  =  df['Next_Val_St_Dt'][0] 
                Val_End_Dt =  df['Next_Val_End_Dt'][0]
                Next_Val_St_Dt  =  df['Next_Val_St_Dt'][0]+timedelta(minutes=duration_min)
                Next_Val_End_Dt =  df['Next_Val_End_Dt'][0]+timedelta(minutes=duration_min)
                print("In Success delta")
            inst_qry = inst_qry.format(object_id, Prev_Val_St_Dt, Prev_Val_End_Dt, Val_St_Dt, Val_End_Dt, Next_Val_St_Dt, Next_Val_End_Dt, Last_Update_Date, Validation_Period, Validation_Frequency, Run_ID, job_id)
            print(inst_qry)
            #cursor.execute(inst_qry)
            client.query(inst_qry)
            # conn.commit()
            
        elif df['Val_Status'][0] == 'Executing':
            Prev_Val_St_Dt  =  df['Prev_Val_St_Dt'][0]
            Prev_Val_End_Dt =  df['Prev_Val_End_Dt'][0]
            Val_St_Dt  =       df['Curr_Val_St_Dt'][0] 
            Val_End_Dt =       df['Curr_Val_End_Dt'][0]
            Next_Val_St_Dt  =  df['Next_Val_St_Dt'][0]
            Next_Val_End_Dt =  df['Next_Val_End_Dt'][0] 
            Run_ID          =  df['Run_Id'][0]
            
        elif df['Val_Status'][0] == 'Failure':
            print("Execution aborted ! ")
            Val_St_Dt  =  df['Curr_Val_St_Dt'][0] 
            Val_End_Dt =  df['Curr_Val_End_Dt'][0]
            #sys.exit(1)

    else:
        print(f"No past records found for object id in {execution_stat_table} Inserting a new one and doing validation for today")
        if Validation_Period == 'full table':       
            date = datetime.utcnow()
            Val_St_Dt = datetime(1900, 1, 1, 0, 0, 0)
            Val_End_Dt = date
            Prev_Val_St_Dt = 'NULL'
            Prev_Val_End_Dt = 'NULL'
            Next_Val_St_Dt = datetime(1900, 1, 1, 0, 0, 0)
            Next_Val_End_Dt = date+timedelta(days=+1)
            print("inside no past record full table")
        else:
            date = datetime.utcnow()            
            Val_End_Dt = date
            Val_St_Dt = Val_End_Dt+timedelta(minutes=-duration_min)
            Prev_Val_St_Dt = 'NULL'
            Prev_Val_End_Dt = 'NULL'
            Next_Val_St_Dt = Val_St_Dt+timedelta(minutes=duration_min)
            Next_Val_End_Dt = Val_End_Dt+timedelta(minutes=duration_min)
            print("inside no past record delta")
            
        inst_qry = f"""INSERT INTO `{DATASET_ID}.{execution_stat_table}` (Object_ID, Validation_Period, Validation_frequency, Run_Type, Prev_Val_St_Dt, Prev_Val_End_Dt, Curr_Val_St_Dt, Curr_Val_End_Dt, Next_Val_St_Dt, Next_Val_End_Dt, Val_Status, Last_Upd_Dt, Run_Id, Job_Id) VALUES
                    ({{0}}, '{{8}}', '{{9}}', 'Automatic', {{1}}, {{2}}, '{{3}}', '{{4}}','{{5}}', '{{6}}', 'Executing', '{{7}}', {{10}}, '{{11}}')"""

        inst_qry = inst_qry.format(object_id, Prev_Val_St_Dt, Prev_Val_End_Dt, Val_St_Dt, Val_End_Dt, Next_Val_St_Dt, Next_Val_End_Dt, Last_Update_Date, Validation_Period, Validation_Frequency, Run_ID, job_id)
        print(inst_qry)
        #cursor.execute(inst_qry)
        client.query(inst_qry)
        # conn.commit()
    return Val_St_Dt, Val_End_Dt, Run_ID

def main():    
    """
    Manages validation flow depending on type of input received.
    Params:
    Returns:
    """
    job_type = str(sys.argv[1]) 
    job_id = sys.argv[3] 
    db_name = sys.argv[1]
    conn = get_connection()
    cursor = conn.cursor()
    if job_type == 'gcs':
        try:
            bucket = sys.argv[3]      # 'datagrokr' 
            filepath = sys.argv[4]    # 'data_validation_stag/account.csv'
            dataset_id = sys.argv[5]  # 'salesforce'
            table_id = sys.argv[6]    # 'account'
            
            qry = """SELECT distinct Failure_Threshold_Value, Criticality, Mapping_ID FROM data_validation_rule_threshold WHERE Active = 1"""
            
            run_id_qry = f"""SELECT max(Object_log_id)+1 as RunId FROM `{DATASET_ID}.{obj_log_table}` WHERE CAST (Last_Update_Date AS date) = current_date and object_id = {{0}}"""
            
            query =  """select a.Object_ID, b.Rule_ID, b.Mapping_ID, a.Object_Name, Primary_Key, Rule_Logic, Column_Name, participating_table, a.Object_Extension,
                        Validation_Period, Time_Interval, Validation_Frequency, c.Rule_Description from data_validation_object_lookup a join data_validation_rule_mapping b on
                        a.Object_ID=b.Object_ID join data_validation_rule c on b.Rule_ID=c.Rule_ID 
                        where a.Object_Database_Name = '{0}' and a.Object_Name = '{1}' and a.Active = 1 and b.Active = 1""".format(dataset_id, table_id)

            print(query)
            cursor.execute(query)
            pdf = pd.DataFrame(cursor.fetchall(), columns = ['Object_ID', 'Rule_ID', 'Mapping_ID','Object_Name', 'Primary_Key', 'Rule_Logic', 'Column_Name', 'participating_table', 'Object_Extension','Validation_Period', 'Time_Interval', 'Validation_Frequency','Rule_Description'])
            print(pdf)
            cursor.execute(qry)
            object_id = pdf['Object_ID'][0]
            run_id_qry = run_id_qry.format(object_id)
            query_job = client.query(run_id_qry)
            results = query_job.result()
            Run_ID = 1            
            try:
                for row in results:
                    print(row)
                    if row.RunId == None:
                        Run_ID = 1
                    else:
                        Run_ID = int(row.RunId)
            except:
                pass
                    
            Validation_Period = str(pdf['Validation_Period'][0]).lower()
            Time_Interval =  pdf['Time_Interval'][0]
            Validation_Frequency = pdf['Validation_Frequency'][0]
            Val_St_Dt, Val_End_Dt, Run_ID = calculate_validation_date(object_id, Time_Interval, Validation_Period, Validation_Frequency, job_id, Run_ID, client)  
                
            Criticality_df = pd.DataFrame(cursor.fetchall(), columns = ['Failure_Threshold_Value', 'Criticality', 'Mapping_ID'])
            if pdf.empty == False:
                total_results, err_agg_df, log_df = validation_custodial.process_gcs(pdf, Run_ID, Criticality_df, job_id, bucket, filepath, PROJECT, dataset_id, table_id, spark, Val_St_Dt, Val_End_Dt, sc, cursor,database)
                source_extract.write_results(total_results, DATASET_ID, output_table, output_dir, job_id, spark)
                source_extract.write_results(err_agg_df, DATASET_ID, output_agg_table, output_dir, job_id, spark)
                source_extract.write_results(log_df, DATASET_ID, obj_log_table, output_dir, job_id, spark)
            else:
                logger.warning('No active rule or table entry found in the Metadata table! Abort')
                
            Last_Update_Date = datetime.utcnow()
            updt_qry = f"""update `{DATASET_ID}.{execution_stat_table}` set Val_Status = "Success" , Last_Upd_Dt = "{Last_Update_Date}" where Object_ID = {object_id} and Run_Id = {Run_ID} and Val_Status = "Executing" """
            print(updt_qry)
            client.query(updt_qry)            
            #notification_alert(object_id, cursor)
            #notification.mail_notification(object_id, cursor)
            #cursor.execute(updt_qry)
            #conn.commit()
                        
        except BaseException as e:
            Last_Update_Date = datetime.utcnow()
            logger.warning('Unable to process gcs file(s)! Aborting... \n' + str(e))
            updt_qry = f"""update `{DATASET_ID}.{execution_stat_table}` set Val_Status = "Failure" , Last_Upd_Dt = "{Last_Update_Date}" and Val_Status = "Executing" """
            print(updt_qry)
            client.query(updt_qry)
            #cursor.execute(updt_qry)
            #conn.commit()
            
    elif job_type == 'cloudsql':
        try:
            object_id = sys.argv[2]
            qry = """SELECT distinct Failure_Threshold_Value, Criticality, Mapping_ID FROM data_validation_rule_threshold WHERE Active = 1"""
            run_id_qry = f"""SELECT max(Object_log_id)+1 as RunId FROM `{DATASET_ID}.{obj_log_table}` WHERE CAST (Last_Update_Date AS date) = current_date and object_id = {{0}}"""
            
            query =  """SELECT distinct a.Object_ID, participating_table as Source_Table, b.Rule_ID, b.Mapping_ID, a.Object_Name,
                            Primary_Key, Rule_Logic, Column_Name, a.Validation_Period, participating_table , b.Source_Table_Date_Col, Validation_Frequency, Time_Interval, Object_Database_Name c.Rule_Description from data_validation_object_lookup a 
                            join data_validation_rule_mapping b on a.Object_ID=b.Object_ID join data_validation_rule c 
                            on b.Rule_ID=c.Rule_ID 
                            where a.Object_ID = {Object_ID}  and b.Test_Type = 'Custodial' and a.Active = 1 and b.Active = 1""".format(Object_ID=object_id)
            
            cursor.execute(query)
            pdf = pd.DataFrame(cursor.fetchall(), columns = ['Object_ID','source_table', 'Rule_ID', 'Mapping_ID','Object_Name', 'Primary_Key','Rule_Logic','Column_Name', 'Validation_Period', 'participating_table', 'Source_Table_Date_Col', 'Validation_Frequency', 'Time_Interval', 'Object_Database_Name','Rule_Description'])
             
            cursor.execute(qry)
            
            
            #value = df_business_rule.iloc[0,13]
            value =  pdf['Object_Database_Name'][0]
            print("My Actual Database Name is ", value)
            
           
            
            if value == sys.argv[4] :
                print ("Object databse name matched with actual Argument")
            if value != sys.argv[4] :
                print(" Object_Database_Name does not match")
                raise BaseException
                
            
            
            
            
            
            

            run_id_qry = run_id_qry.format(object_id)
            query_job = client.query(run_id_qry)
            results = query_job.result()
            Run_ID = 1            
            try:
                for row in results:
                    print(row)
                    if row.RunId == None:
                        Run_ID = 1
                    else:
                        Run_ID = int(row.RunId)
            except:
                pass

            Criticality_df = pd.DataFrame(cursor.fetchall(), columns = ['Failure_Threshold_Value', 'Criticality', 'Mapping_ID','Object_Name','Rule_Description'])
            if pdf.empty == False:                
                Validation_Period = str(pdf['Validation_Period'][0]).lower()
                Time_Interval =  pdf['Time_Interval'][0]
                Validation_Frequency = pdf['Validation_Frequency'][0]
                Val_St_Dt, Val_End_Dt, Run_ID = calculate_validation_date(object_id, Time_Interval, Validation_Period, Validation_Frequency, job_id, Run_ID, client)  
                
                total_results, err_agg_df, log_df = process_cloudsql(pdf, job_id, Criticality_df, hostip, DATASET_ID, user, pswd, PROJECT, spark, Run_ID, Val_St_Dt, Val_End_Dt, cursor, sc,database)
                write_results(total_results, DATASET_ID, output_table, output_dir, job_id, spark)
                write_results(err_agg_df, DATASET_ID, output_agg_table, output_dir, job_id, spark)
                write_results(log_df, DATASET_ID, obj_log_table, output_dir, job_id, spark)
            else:
                 logger.warning('No active rule or table entry found in the Metadata table! Abort')
            Last_Update_Date = datetime.utcnow()
            updt_qry = f"""update `{DATASET_ID}.{execution_stat_table}` set Val_Status = "Success" , Last_Upd_Dt = "{Last_Update_Date}" where Object_ID = {object_id} and Val_Status = 'Executing'"""
            print(updt_qry)
            client.query(updt_qry)
            #notification_alert(object_id, cursor)            
            #notification.mail_notification(object_id, cursor)
            #cursor.execute(updt_qry)
            #conn.commit()
            
            
        except BaseException as e:
            Last_Update_Date = datetime.utcnow()
            logger.warning('Unable to process cloudsql! Aborting... \n' + str(e))
            updt_qry = f"""update `{DATASET_ID}.{execution_stat_table}` set Val_Status = "Failure" , Last_Upd_Dt = "{Last_Update_Date}" where Object_ID = {object_id} and Val_Status = 'Executing'"""
            client.query(updt_qry)
            print(updt_qry)
            #cursor.execute(updt_qry)
            #conn.commit()
            

    elif job_type == 'bigquery':
        try:
            object_id = sys.argv[2]

            
            qry = """SELECT distinct Failure_Threshold_Value, Criticality, Mapping_ID FROM data_validation_rule_threshold WHERE Active = 1"""
            run_id_qry = f"""SELECT max(Object_log_id)+1 as RunId FROM `{DATASET_ID}.{obj_log_table}` WHERE CAST (Last_Update_Date AS date) = current_date and object_id = {{0}}"""
            query =  """select distinct a.Object_ID, Object_Name as source_table, b.Rule_ID, b.Mapping_ID, a.Object_Name,
                        Primary_Key,Rule_Logic,Column_Name, a.Validation_Period, participating_table, Time_Interval, Source_Table_Date_Col, Validation_Frequency, Object_Database_Name, c.Rule_Description from data_validation_object_lookup a 
                        join data_validation_rule_mapping b on a.Object_ID=b.Object_ID join data_validation_rule c 
                        on b.Rule_ID=c.Rule_ID join data_validation_rule_threshold d on b.Mapping_ID=d.Mapping_ID 
                        where a.Object_ID = {0} and b.Test_Type = 'Business' and a.Active = 1 and b.Active = 1 """.format(object_id)

            cursor.execute(query)
            print(query)
            
            df_business_rule = pd.DataFrame(cursor.fetchall(), columns = ['Object_ID', 'source_table', 'Rule_ID', 'Mapping_ID','Object_Name', 'Primary_Key', 'Rule_Logic', 'Column_Name', 'Validation_Period', 'participating_table', 'Time_Interval', 'Source_Table_Date_Col', 'Validation_Frequency', 'Object_Database_Name','Rule_Description'])
            
            print("System arguments",sys.argv[4])
            #value = df_business_rule.iloc[0,13]
            value = df_business_rule['Object_Database_Name'][0]
            print("My Actual Database Name is ", value)
            
            if value == sys.argv[4] :
                print ("Object Data_Name matched with actual Argument")
            if value != sys.argv[4] :
                print(" Object_Database_Name does not match with" ,value )
                raise BaseException 
                
                
                
                
            print(df_business_rule)
            run_id_qry = run_id_qry.format(object_id)
            print(run_id_qry)
            query_job = client.query(run_id_qry)
            results = query_job.result()
            Run_ID = 1
            try:
                for row in results:
                    print(row)
                    if row.RunId == None:
                        Run_ID = 1
                    else:
                        Run_ID = int(row.RunId)
            except:
                pass
            cursor.execute(qry)
            Criticality_df = pd.DataFrame(cursor.fetchall(), columns = ['Failure_Threshold_Value', 'Criticality', 'Mapping_ID'])


            query = """select distinct a.Object_ID, Object_Name as source_table, b.Rule_ID, b.Mapping_ID, a.Object_Name,
                            Primary_Key, Rule_Logic, Column_Name, a.Validation_Period, participating_table ,Time_Interval, Source_Table_Date_Col, Validation_Frequency, Object_Database_Name, c.Rule_Description from data_validation_object_lookup a 
                            join data_validation_rule_mapping b on a.Object_ID=b.Object_ID join data_validation_rule c 
                            on b.Rule_ID=c.Rule_ID join data_validation_rule_threshold d on b.Mapping_ID=d.Mapping_ID 
                            where a.Object_ID = {0}  and b.Test_Type = 'Custodial' and b.Active = 1""".format(object_id)

            cursor.execute(query)
            df_custodial_rule = pd.DataFrame(cursor.fetchall(), columns = ['Object_ID','source_table', 'Rule_ID', 'Mapping_ID','Object_Name', 'Primary_Key','Rule_Logic','Column_Name', 'Validation_Period', 'participating_table', 'Time_Interval', 'Source_Table_Date_Col', 'Validation_Frequency', 'Object_Database_Name','Rule_Description'])
            print(df_custodial_rule)
            if df_business_rule.empty != True:                 
                Validation_Period = str(df_business_rule['Validation_Period'][0]).lower()
                Time_Interval =  df_business_rule['Time_Interval'][0]                
                Validation_Frequency = df_business_rule['Validation_Frequency'][0]
                #t1 = datetime.now()
                Val_St_Dt, Val_End_Dt, Run_ID = calculate_validation_date(object_id, Time_Interval, Validation_Period, Validation_Frequency, job_id, Run_ID, client)
                business_result_df, business_err_agg_df, business_log_df = val_exe_tbl(df_business_rule, Criticality_df, Run_ID,  client, spark, PROJECT, sc, Val_St_Dt, Val_End_Dt, str(DATASET_ID+'.'+output_table), cursor)

                write_results(business_result_df, DATASET_ID, output_table, output_dir, job_id, spark)
                write_results(business_err_agg_df, DATASET_ID, output_agg_table, output_dir, job_id, spark)
                write_results(business_log_df, DATASET_ID, obj_log_table, output_dir, job_id, spark)
                notification_alert(business_err_agg_df, cursor)
                
            if df_custodial_rule.empty != True:
                Validation_Period = str(df_custodial_rule['Validation_Period'][0]).lower()
                Time_Interval =  df_custodial_rule['Time_Interval'][0]
                Validation_Frequency = df_custodial_rule['Validation_Frequency'][0]
                
                Val_St_Dt, Val_End_Dt, Run_ID = calculate_validation_date(object_id, Time_Interval, Validation_Period, Validation_Frequency, job_id, Run_ID, client)
                custodial_result_df, custodial_err_agg_df , custodial_log_df = process_bigquery(df_custodial_rule, Criticality_df, Run_ID, spark, PROJECT, sc, Val_St_Dt, Val_End_Dt, cursor,database)
                
                write_results(custodial_result_df, DATASET_ID, output_table, output_dir, job_id, spark)
                write_results(custodial_err_agg_df, DATASET_ID, output_agg_table, output_dir, job_id, spark)
                write_results(custodial_log_df, DATASET_ID, obj_log_table, output_dir, job_id, spark)
                notification_alert(custodial_err_agg_df, cursor)
            if (df_custodial_rule.empty == True and df_business_rule.empty == True):
                logger.warning('No active rule or table entry found in the Metadata table! Abort')        
            Last_Update_Date = datetime.utcnow()
            updt_qry = f'update `{DATASET_ID}.{execution_stat_table}` set Val_Status = "Success" , Last_Upd_Dt = "{Last_Update_Date}" where Object_ID = {object_id} and Run_Id = {Run_ID} and Val_Status = "Executing"'
            print(updt_qry)
            client.query(updt_qry)
            #notification_alert(object_id, cursor)
            #notification.mail_notification(object_id, cursor)
            #cursor.execute(updt_qry)
            #conn.commit()            
            
        except BaseException as e:
            Last_Update_Date = datetime.utcnow()
            logger.warning('Unable to process bigquery table! Aborting... \n' + str(e))
            updt_qry = f'update `{DATASET_ID}.{execution_stat_table}` set Val_Status = "Failure" , Last_Upd_Dt = "{Last_Update_Date}" where Object_ID = {object_id} and Val_Status = "Executing"'
            print(updt_qry)
            #cursor.execute(updt_qry)
            #client.query(updt_qry)           
main()