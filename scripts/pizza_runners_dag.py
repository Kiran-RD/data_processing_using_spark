from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'krd',
    'depends_on_past': False,
    'email': ['kiran.r.diggavi@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'template_searchpath' : ['/home/saif/airflow/dags/pizza_runners_dag.py']
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'Pizza_Runner',
    default_args=default_args,
    description='Pizza Runner Tasks Scheduler',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
)

start = DummyOperator(task_id='start', dag=dag)

t1 = BashOperator(
    task_id='Setup_Mysql_Database',
    bash_command='''mysql -uroot -pWelcome@123 --local-infile=1 -e"source /home/saif/LFS/cohort_c8/Usecase_2/pizza_runner_mysql_setup.sql"''',
    dag=dag
)

t2 = BashOperator(
    task_id='Setup_Hive_Database',
    bash_command='''hive -f /home/saif/LFS/cohort_c8/Usecase_2/pizza_runners_hive_setup.hql''',
    dag=dag
)

t3 = BashOperator(
    task_id='Sqoop_Imports',
    # bash_command='/home/saif/LFS/cohort_c8/Usecase_2/pizza_runners_sqoop_commands.sh',
    bash_command = """
    sqoop import \
    --connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
    --username root \
    --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
    --table runners \
    --columns 'runner_id,registration_date' \
    -m 1 \
    --target-dir /user/hive/warehouse/pizza_runners.db/runners \
    --delete-target-dir

    sqoop import \
    --connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
    --username root \
    --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
    --table pizza_names \
    --columns 'pizza_id,pizza_name' \
    -m 1 \
    --target-dir /user/hive/warehouse/pizza_runners.db/pizza_names \
    --delete-target-dir

    sqoop import \
    --connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
    --username root \
    --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
    --table pizza_toppings \
    --columns 'topping_id,topping_name' \
    -m 1 \
    --target-dir /user/hive/warehouse/pizza_runners.db/pizza_toppings \
    --delete-target-dir

    sqoop import \
    --connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
    --username root \
    --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
    --table runner_orders \
    --columns 'order_id,runner_id,pickup_time,distance,duration,cancellation' \
    -m 1 \
    --target-dir /user/hive/warehouse/pizza_runners.db/runner_orders \
    --delete-target-dir

    sqoop import \
    --connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
    --username root \
    --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
    --query "select order_id,customer_id,pizza_id,replace(exclusions,',','|') exclusions,replace(extras,',','|') extras,order_time from customer_orders where \$CONDITIONS" \
    -m 1 \
    --target-dir /user/hive/warehouse/pizza_runners.db/customer_orders \
    --delete-target-dir

    sqoop import \
    --connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
    --username root \
    --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
    --query "select pizza_id,replace(toppings,',','|') from pizza_recipes where \$CONDITIONS" \
    -m 1 \
    --target-dir /user/hive/warehouse/pizza_runners.db/pizza_recipes \
    --delete-target-dir
    """,
    dag=dag
)

# t4 = SparkSubmitOperator(
#                         task_id='Run_Spark',
#                         application='/home/saif/LFS/cohort_c8/Usecase_2/pizza_runners_spark_solutions.sh',
#                         conn_id='yarn',
#                         driver_memory='1G',
#                         executor_memory='2G',
#                         num_executors=1,
#                         executor_cores=4,
#                         dag=dag
#                         )

t4 = BashOperator(
    task_id='Run_Spark',
    bash_command='python3 /home/saif/LFS/cohort_c8/Usecase_2/pizza_runners_spark_solutions.py',
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> t1 >> t2 >> t3 >> t4 >> end
