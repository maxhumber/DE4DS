from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

from hockey_reference import fetch_player

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('hockey', default_args=default_args, schedule_interval=timedelta(days=1))

# 1 fetch new data (fake date)
# hockey_refernce.py

# 2 train new model
# model_rollbar.py

# predict and add predictions to sql
# predict_to_database.py

t1 = PythonOperator(
    task_id='fetch_player',
    provide_context=True,
    python_callable=fetch_player,
    op_kwargs={'player_id': 'ovechal01', 'date': 2019-02-17'}
    dag=dag,
)

# # fetch new data
# t1 = BashOperator(
#     task_id='fetch data',
#     bash_command='python hockey',
#     dag=dag)
#
# t2 = BashOperator(
#     task_id='sleep',
#     bash_command='sleep 5',
#     retries=3,
#     dag=dag)
#
# templated_command = """
#     {% for i in range(5) %}
#         echo "{{ ds }}"
#         echo "{{ macros.ds_add(ds, 7)}}"
#         echo "{{ params.my_param }}"
#     {% endfor %}
# """
#
# t3 = BashOperator(
#     task_id='templated',
#     bash_command=templated_command,
#     params={'my_param': 'Parameter I passed in'},
#     dag=dag)
#
# t2.set_upstream(t1)
# t3.set_upstream(t1)
