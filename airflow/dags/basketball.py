import sqlite3
import sys
from datetime import datetime, timedelta

import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

HOME = "/Users/max/Courses/DE4DS"
sys.path.append(HOME)  # needed for custom imports

from data.scrape import get_games

# setup
default_args = {
    "owner": "Max",
    "start_date": datetime(2022, 05, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("basketball", default_args=default_args, schedule_interval=timedelta(days=1))

# customize function to accept context
def fetch(**context):
    date = context["execution_date"].strftime("%Y-%m-%d")
    # date = "2022-05-18"
    df = get_games(date)
    con = sqlite3.connect(f"{HOME}/data/football.db")
    df.to_sql("players", con, if_exists="append", index=False)
    con.close()


# the actual tasks
t1 = PythonOperator(
    task_id="fetch", python_callable=fetch, provide_context=True, dag=dag
)

t2 = BashOperator(
    task_id="model", bash_command=f"cd {HOME}; python 07-continuous.py", dag=dag
)

# order the tasks (set t2 downstream)
t1 >> t2
