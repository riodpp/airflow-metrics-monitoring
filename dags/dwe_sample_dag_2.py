
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'dwe',
    'depends_on_past': False,
    'email': ['dwe@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'dwe_sample_dag_2',
    access_control = {
        'DWE' : {'can_edit', 'can_read'},
        'CDE' : {'can_read'}
    },
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    t1 >> t2

    affinity_kpo = k8s.V1Affinity(
        node_affinity=k8s.V1NodeAffinity(
            preferred_during_scheduling_ignored_during_execution=[
                k8s.V1PreferredSchedulingTerm(
                    weight=1,
                    preference=k8s.V1NodeSelectorTerm(
                        match_expressions=[
                            k8s.V1NodeSelectorRequirement(key="key", operator="In", values=["dwe-airflow-main"])
                        ]
                    ),
                )
            ]
        )
    )

    tolerations_kpo = [k8s.V1Toleration(key="dedicated", operator="Equal", value="dwe-dags")]

    kpo_test_train = KubernetesPodOperator(
        dag=dag,
        task_id='l3_fact_train_search',
        name='l3_fact_train_search',
        is_delete_operator_pod=True,
        queue='kubernetes',
        namespace="tvlk-data-train-prod",
        image=f"gcr.io/tvlk-data-bqmartpoc-dev/dwebt_image",
        cmds=["dbt","run","-m","fact_train_search"],
        image_pull_policy='Always',
        service_account_name='tvlk-data-train-prod',
        affinity=affinity_kpo,
        tolerations=tolerations_kpo
    )

    kpo_test_rental = KubernetesPodOperator(
        dag=dag,
        task_id='l2_rental_frontend_visit',
        name='l2_rental_frontend_visit',
        is_delete_operator_pod=True,
        queue='kubernetes',
        namespace="tvlk-data-rental-prod",
        image=f"gcr.io/tvlk-data-bqmartpoc-dev/dwebt_image",
        cmds=["dbt","run","-m","rental_frontend_visit"],
        image_pull_policy='Always',
        service_account_name='tvlk-data-rental-prod',
        affinity=affinity_kpo,
        tolerations=tolerations_kpo
    )