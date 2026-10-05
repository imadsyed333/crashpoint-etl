from airflow import DAG
from airflow.operators.python import PythonOperator

from src.pipeline import download, load_collisions, load_intersections, load_addresses, split_collisions, geocode_collisions
from src.run import load_to_postgis

with DAG(
    'geocode_pipeline',
    default_args={
        'owner': 'admin',
    },
) as dag:
    
    download_task = PythonOperator(
        task_id='download',
        python_callable=download,
    )

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

    load_to_postgis_task = PythonOperator(
        task_id='load_to_postgis',
        python_callable=load_to_postgis,
    )

    download_task >> [load_collisions_task, load_intersections_task, load_addresses_task] >> split_collisions_task >> geocode_collisions_task >> load_to_postgis_task
