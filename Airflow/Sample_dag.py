from airflow import DAG
# from airflow.operators import BashOperator, PythonOperator, BranchPythonOperator

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from random import randint
from datetime import datetime, timedelta

'''
   accuracies = ti.xcom_pull(task_ids=['training_model_a',
                                        'training_model_b',
                                        'training_model_c'])
                                        '''


# ti.xcom_pull(task_ids=kwargs['task_name'])



def _training_model_():
    return randint(1, 10)


def _check_accuracy_(ti, **kwargs):
    # xcom used to cross communicate between tasks

    accuracies = int(ti.xcom_pull(task_ids=kwargs['training_model_a']))

    #best_accuracy = max(accuracies)
    best_accuracy = accuracies

    if best_accuracy > 8:
        return 'accurate'
    else:
        return 'inaccurate'


with DAG("my_dag", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    training_model_a = PythonOperator(
        task_id="dice_roll_1",
        python_callable=_training_model_
        # xcom_push=True

    )

    training_model_b = PythonOperator(
        task_id="dice_roll_2",
        python_callable=_training_model_
        # xcom_push=True

    )

    training_model_c = PythonOperator(
        task_id="dice_roll_3",
        python_callable=_training_model_
        # xcom_push=True
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_check_accuracy_

    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

[training_model_a, training_model_b, training_model_c] >> choose_best_model >> [accurate, inaccurate]
