import sys

HOME = "/Users/max/Repos/DE4DS"
sys.path.append(HOME)  # needed for custom imports

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import pandas as pd
import sqlite3

from scrape import fetch_player

default_args = {
    "owner": "Max",
    "start_date": datetime(2019, 7, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("hockey", default_args=default_args, schedule_interval=timedelta(days=1))

# customize function to accept context
def fetch(**context):
    date = context["execution_date"].strftime("%Y-%m-%d")
    players = [
        "nylanwi01",  # willy
        "ovechal01",  # ovi
        "mcdavco01",  # mcjesus
        "matthau01",  # austen
    ]
    data = pd.DataFrame()
    for player_id in players:
        di = fetch_player(player_id, date)
        data = data.append(di)
    con = sqlite3.connect(f"{HOME}/data/hockey.db")
    data.to_sql("players", con, if_exists="append", index=False)
    con.close()


# the actual tasks
t1 = PythonOperator(
    task_id="fetch", python_callable=fetch, provide_context=True, dag=dag
)

t2 = BashOperator(
    task_id="model", bash_command=f"cd {HOME}; python model_continuous.py", dag=dag
)

# order the tasks (set t2 downstream)
t1 >> t2
