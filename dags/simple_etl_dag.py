import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from pymongo import MongoClient,InsertOne
from datetime import datetime,timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))

from scripts.mockup_data import generate_mockup_data
from scripts.transflow_ipl_data import mongo_transform, mysql_transform

dag = DAG(dag_id='simple_etl_dag',start_date=datetime(2025,5,1),schedule_interval=None,catchup=False)


def _determine_branch(**kwargs):
    # Check upstream task states using trigger rules
    ti = kwargs['ti']
    
    upstream_task_ids = ['mysql_task','mongo_task']  
    
    for task_id in upstream_task_ids:
        task_state = ti.xcom_pull(task_ids=task_id, key='return_value')
        print(f"Task {task_id} state: {task_state}")
        if task_state == 'failed':  
            return 'failure_path'
    return 'success_path'

success_notification = SlackWebhookOperator(
        task_id='success_path',
        slack_webhook_conn_id='slack_default',
        message="""
            :white_check_mark: *Success Notification*
            *DAG*: {{ dag.dag_id }}
            *Task*: {{ task.task_id }}
            *Execution Time*: {{ ts }}
            *Log URL*: {{ ti.log_url }}
        """,
        channel='#airflow-notifications',
        username='airflow-bot',
        icon_emoji=':airflow:',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )


failure_notification = SlackWebhookOperator(
        task_id='failure_path',
        slack_webhook_conn_id='slack_default',
        message="""
            :x: *Failure Notification*
            *DAG*: {{ dag.dag_id }}
            *Task*: {{ task.task_id }}
            *Execution Time*: {{ ts }}
            *Log URL*: {{ ti.log_url }}
            *Exception*: {{ ti.exception }}
        """,
        username='airflow-bot',
        channel='#airflow-notifications',
        dag=dag,
        icon_emoji=':airflow:',
        trigger_rule=TriggerRule.ONE_FAILED,
    )


def transform_data(**kwargs):
    file_path = '/opt/airflow/data/mockup_data.csv'
    mysql_records = mysql_transform(file_path)
    mongo_records = mongo_transform(file_path)

    if mysql_records is None or mongo_records is None:
        raise ValueError("Transformation failed. No records to process.")
    
    # Push transformed data to XCom
    kwargs['ti'].xcom_push(key='mysql_records', value=mysql_records)
    kwargs['ti'].xcom_push(key='mongo_records', value=mongo_records)


def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id='minio_bucket_connection')
    bucket_name = 'backup'
    file_path = '/opt/airflow/data/mockup_data.csv'
    key = os.path.basename(file_path)

    # Check if bucket exists
    if s3_hook.check_for_bucket(bucket_name):
        print(f"Bucket {bucket_name} exists. Checking for files...")
        
        # List all files in the bucket
        files = s3_hook.list_keys(bucket_name)
        
        if files:
            print(f"Bucket contains {len(files)} files. Deleting them...")
            # Delete all files in the bucket
            for file_key in files:
                s3_hook.delete_objects(bucket_name, file_key)
        
        print(f"Deleting bucket {bucket_name}...")
        s3_hook.delete_bucket(bucket_name)
    
    # Create new bucket
    print(f"Creating new bucket {bucket_name}...")
    s3_hook.create_bucket(bucket_name)
    
    # Upload file
    print(f"Uploading file {file_path} to bucket {bucket_name}...")
    s3_hook.load_file(
        filename=file_path,
        bucket_name=bucket_name,
        key=key,
        replace=True
    )
    print("Upload complete!")

def load_to_mysql(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    mysql_records = kwargs['ti'].xcom_pull(key='mysql_records', task_ids='transform_task')
    
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS cricket_matches")
    # Create table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS cricket_matches (
        match_id INT PRIMARY KEY,
        captain VARCHAR(100),
        winner VARCHAR(50),
        toss_winner VARCHAR(50),
        venue VARCHAR(100),
        date_of_match VARCHAR(50),
        toss_decision_impact TINYINT
    )
    """
    cursor.execute(create_table_sql)

    for record in mysql_records:
        insert_sql = """
        INSERT INTO cricket_matches (
            match_id, captain, winner, toss_winner, venue, 
            date_of_match, toss_decision_impact
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            captain = VALUES(captain),
            winner = VALUES(winner),
            toss_winner = VALUES(toss_winner),
            venue = VALUES(venue),
            date_of_match = VALUES(date_of_match),
            toss_decision_impact = VALUES(toss_decision_impact)        """
        cursor.execute(insert_sql, (
            record['match_id'],
            record['captain'],
            record['winner'],
            record['toss_winner'],
            record['venue'],
            record['date_of_match'],
            record['toss_decision_impact'],
        ))
    
    conn.commit()
    print(f"Loaded {len(mysql_records)} records into MySQL")

    
def load_to_monogo(**kwargs):
    ti = kwargs['ti']
    mongo_records = ti.xcom_pull(task_ids='transform_task', key='mongo_records')
    client = MongoClient("mongodb://mongoadmin:secret@mongo:27017/testdb?authSource=admin")
    db = client['testdb']
    collection = db['test_collection']
    operations = [InsertOne(record) for record in mongo_records]

    try:
        # Perform bulk write operation
        if operations:  # Only execute if there are operations
            result = collection.bulk_write(operations)
            print(f"Successfully inserted {len(mongo_records)} records into MongoDB")
        else:
            print("No records to insert")
            
    except Exception as e:
        print(f"Error loading to MongoDB: {str(e)}")
        raise
    finally:
        client.close()




start_task = EmptyOperator(task_id='start_task',dag=dag)


slack_success_notification = SlackWebhookOperator(
    task_id='slack_success_notification',
    slack_webhook_conn_id='slack_webhook',
    message='ETL DAG completed successfully!',
    channel='#airflow-notifications',)  

extract_task = PythonOperator(task_id='extract_task',python_callable=generate_mockup_data,op_kwargs={'num_of_data':2000,'file_name':'/opt/airflow/data/mockup_data.csv'},dag=dag)

file_sensor_task = FileSensor(task_id='file_sensor_task',
                              fs_conn_id='fs_connection_id',
                              filepath='/opt/airflow/data/mockup_data.csv',
                              poke_interval=15,
                              timeout=60,
                              mode='poke',
                              dag=dag)

copy_to_s3_task = PythonOperator(task_id='copy_to_s3_task',python_callable=upload_to_s3,dag=dag)



transform_task = PythonOperator(task_id='transform_task',python_callable=transform_data,provide_context=True,dag=dag)

mysql_task = PythonOperator(task_id='mysql_task',python_callable=load_to_mysql,provide_context=True,dag=dag)

mongo_task = PythonOperator(task_id='mongo_task',python_callable=load_to_monogo,provide_context=True,dag=dag)

branch = BranchPythonOperator(
        task_id='determine_notification_path',
        python_callable=_determine_branch,
        provide_context=True,
    )

end_task = EmptyOperator(task_id='end_task',dag=dag,trigger_rule=TriggerRule.ALL_DONE)

start_task >> extract_task >> file_sensor_task >> copy_to_s3_task >> transform_task  >> [ mysql_task , mongo_task ] >> branch 
branch  >>  [success_notification, failure_notification] >> end_task