from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from src.pipeline import load_collisions, load_intersections, load_addresses, split_collisions, geocode_collisions, compute_match_scores

with DAG(
    'geocode_pipeline',
    default_args={
        'owner': 'admin',
    },
) as dag:
    
    load_collisions_task = PythonOperator(
        task_id='load_collisions',
        python_callable=load_collisions,
    )

    load_intersections_task = PythonOperator(
        task_id='load_intersections',
        python_callable=load_intersections,
    )

    load_addresses_task = PythonOperator(
        task_id='load_addresses',
        python_callable=load_addresses,
    )

    split_collisions_task = PythonOperator(
        task_id='split_collisions',
        python_callable=split_collisions,
    )

    geocode_collisions_task = PythonOperator(
        task_id='geocode_collisions',
        python_callable=geocode_collisions,
    )

    compute_match_scores_task = PythonOperator(
        task_id='compute_match_scores',
        python_callable=compute_match_scores,
    )

    [load_collisions_task, load_intersections_task, load_addresses_task] >> split_collisions_task >> geocode_collisions_task >> compute_match_scores_task
