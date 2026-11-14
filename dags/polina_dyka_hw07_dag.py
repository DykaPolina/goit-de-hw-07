"""goit-de-hw-07: підрахунок медалей з розгалуженням, затримкою та сенсором.

create_table -> pick_medal -> pick_medal_task -> calc_{Bronze|Silver|Gold}
             -> generate_delay -> check_for_correctness

Затримка задається параметром запуску `delay_seconds` (Trigger DAG w/ config):
  {"delay_seconds": 5}   -> сенсор успішний (запис молодший за 30 с)
  {"delay_seconds": 35}  -> сенсор падає (запис старший за 30 с)
"""

import random
import time
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.trigger_rule import TriggerRule

CONNECTION_ID = "goit_mysql_db"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"
RESULTS_TABLE = "olympic_dataset.polina_dyka_hw_dag_results"
MEDALS = ["Bronze", "Silver", "Gold"]
MAX_AGE_SECONDS = 30


def pick_medal():
    medal = random.choice(MEDALS)
    print(f"Picked medal: {medal}")
    return medal  # потрапляє в XCom


def pick_medal_branch(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay(params):
    seconds = int(params["delay_seconds"])
    print(f"Sleeping for {seconds} s")
    time.sleep(seconds)


with DAG(
    dag_id="polina_dyka_hw07_medals",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    params={"delay_seconds": Param(5, type="integer", minimum=0)},
    tags=["polina_dyka", "hw07"],
) as dag:

    # 1. Таблиця для результатів
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=CONNECTION_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10) NOT NULL,
            `count` INT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # 2. Випадковий вибір медалі
    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    # 3. Розгалуження за обраним значенням
    branch_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=pick_medal_branch,
    )

    # 4. Три завдання підрахунку (виконується лише обране)
    calc_tasks = [
        MySqlOperator(
            task_id=f"calc_{medal}",
            mysql_conn_id=CONNECTION_ID,
            sql=f"""
            INSERT INTO {RESULTS_TABLE} (medal_type, `count`, created_at)
            SELECT '{medal}', COUNT(*), NOW()
            FROM {SOURCE_TABLE}
            WHERE medal = '{medal}';
            """,
        )
        for medal in MEDALS
    ]

    # 5. Затримка: запускається, якщо одне з calc-завдань успішне,
    #    а два інші пропущені (skipped)
    delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 6. Сенсор: найновіший запис не старший за 30 с.
    #    created_at і NOW() беруться з годинника MySQL, тож часові пояси збігаються.
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONNECTION_ID,
        sql=f"""
        SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= {MAX_AGE_SECONDS}
        FROM {RESULTS_TABLE};
        """,
        mode="poke",
        poke_interval=5,
        timeout=15,
    )

    create_table >> pick_medal_task >> branch_task >> calc_tasks >> delay_task >> check_for_correctness
