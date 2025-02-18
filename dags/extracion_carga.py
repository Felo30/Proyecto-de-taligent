from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime

def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')  # 
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS pacientes (
        id SERIAL PRIMARY KEY,
        nombre VARCHAR(255) NOT NULL,
        fecha_ingreso DATE NOT NULL,
        diagnostico VARCHAR(255),
        tratamiento VARCHAR(255),
        costo_tratamiento NUMERIC,
        seguimiento VARCHAR(100),
        data_quality_status TEXT
    );

    CREATE TABLE IF NOT EXISTS doctores (
        id SERIAL PRIMARY KEY,
        nombre VARCHAR(255) NOT NULL,
        especialidad VARCHAR(255) NOT NULL,
        data_quality_status TEXT
    );

    CREATE TABLE IF NOT EXISTS consultas (
        id SERIAL PRIMARY KEY,
        paciente_id INT REFERENCES pacientes(id),
        doctor_id INT REFERENCES doctores(id),
        fecha_consulta DATE NOT NULL,
        notas TEXT,
        data_quality_status TEXT
    );
    """
    pg_hook.run(create_table_sql)

def extract_data_pacientes():
    df = pd.read_csv('/opt/airflow/data_csv/pacientes.csv', delimiter=",")
    return df.to_dict(orient='records')

def transform_data_pacientes(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='extract_data_pacientes')
    return [
        {
            'nombre': r['nombre_paciente'].upper(),
            'fecha_ingreso': r['fecha_ingreso'],
            'diagnostico': r['diagnostico'],
            'tratamiento': r['tratamiento'],
            'costo_tratamiento': r['costo_tratamiento'],
            'seguimiento': r['seguimiento']
        } for r in records
    ]

def load_data_pacientes(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_data_pacientes')
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')  # 🔄 Conexión corregida
    pg_hook.insert_rows(
        table='pacientes',
        rows=[(
            r['nombre'],
            r['fecha_ingreso'],
            r['diagnostico'],
            r['tratamiento'],
            r['costo_tratamiento'],
            r['seguimiento']
        ) for r in records],
        target_fields=['nombre', 'fecha_ingreso', 'diagnostico', 'tratamiento', 'costo_tratamiento', 'seguimiento']
    )

def extract_data_doctores():
    df = pd.read_csv('/opt/airflow/data_csv/doctores.csv', delimiter=",")
    return df.to_dict(orient='records')

def transform_data_doctores(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='extract_data_doctores')
    return [{'nombre': r['nombre'].upper(), 'especialidad': r['especialidad']} for r in records]

def load_data_doctores(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_data_doctores')
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')  # 🔄 Conexión corregida
    pg_hook.insert_rows(
        table='doctores',
        rows=[(
            r['nombre'],
            r['especialidad']
        ) for r in records],
        target_fields=['nombre', 'especialidad']
    )

def extract_data_consultas():
    df = pd.read_csv('/opt/airflow/data_csv/consultas.csv', delimiter=",")
    return df.to_dict(orient='records')

def transform_data_consultas(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='extract_data_consultas')
    return [
        {
            'paciente_id': r['paciente_id'],
            'doctor_id': r['doctor_id'],
            'fecha_consulta': r['fecha_consulta'],
            'notas': r['notas']
        } for r in records
    ]

def load_data_consultas(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_data_consultas')
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')  # 🔄 Conexión corregida
    pg_hook.insert_rows(
        table='consultas',
        rows=[(
            r['paciente_id'],
            r['doctor_id'],
            r['fecha_consulta'],
            r['notas']
        ) for r in records],
        target_fields=['paciente_id', 'doctor_id', 'fecha_consulta', 'notas']
    )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 13),
    'retries': 1,
}

dag = DAG(
    dag_id='etl_postgres',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Definir tareas en el DAG
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

extract_pacientes = PythonOperator(
    task_id='extract_data_pacientes',
    python_callable=extract_data_pacientes,
    dag=dag
)

transform_pacientes = PythonOperator(
    task_id='transform_data_pacientes',
    python_callable=transform_data_pacientes,
    provide_context=True,
    dag=dag
)

load_pacientes = PythonOperator(
    task_id='load_data_pacientes',
    python_callable=load_data_pacientes,
    provide_context=True,
    dag=dag
)

extract_doctores = PythonOperator(
    task_id='extract_data_doctores',
    python_callable=extract_data_doctores,
    dag=dag
)

transform_doctores = PythonOperator(
    task_id='transform_data_doctores',
    python_callable=transform_data_doctores,
    provide_context=True,
    dag=dag
)

load_doctores = PythonOperator(
    task_id='load_data_doctores',
    python_callable=load_data_doctores,
    provide_context=True,
    dag=dag
)

extract_consultas = PythonOperator(
    task_id='extract_data_consultas',
    python_callable=extract_data_consultas,
    dag=dag
)

transform_consultas = PythonOperator(
    task_id='transform_data_consultas',
    python_callable=transform_data_consultas,
    provide_context=True,
    dag=dag
)

load_consultas = PythonOperator(
    task_id='load_data_consultas',
    python_callable=load_data_consultas,
    provide_context=True,
    dag=dag
)

# Definir la secuencia de ejecución del DAG
create_table_task >> [extract_pacientes, extract_doctores]
extract_pacientes >> transform_pacientes >> load_pacientes
extract_doctores >> transform_doctores >> load_doctores
[load_pacientes, load_doctores] >> extract_consultas >> transform_consultas >> load_consultas
