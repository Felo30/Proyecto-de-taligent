from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def check_and_update_data_quality():
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh') 

    # Verificar calidad de la tabla pacientes
    sql_check_pacientes = """
    SELECT COUNT(*) FROM pacientes 
    WHERE nombre IS NULL OR fecha_ingreso IS NULL OR costo_tratamiento <= 0;
    """
    pacientes_invalid = pg_hook.get_first(sql_check_pacientes)[0]
    status_pacientes = f"Error: {pacientes_invalid} registros inválidos encontrados" if pacientes_invalid > 0 else "Calidad de Dato Excelente"

    sql_update_pacientes = f"""
    UPDATE pacientes SET data_quality_status = '{status_pacientes}';
    """
    pg_hook.run(sql_update_pacientes, autocommit=True)

    # Verificar calidad de la tabla doctores
    sql_check_doctores = """
    SELECT COUNT(*) FROM doctores 
    WHERE nombre IS NULL OR especialidad IS NULL;
    """
    doctores_invalid = pg_hook.get_first(sql_check_doctores)[0]
    status_doctores = f"Error: {doctores_invalid} registros inválidos encontrados" if doctores_invalid > 0 else "Calidad de Dato Excelente"

    sql_update_doctores = f"""
    UPDATE doctores SET data_quality_status = '{status_doctores}';
    """
    pg_hook.run(sql_update_doctores, autocommit=True)

    # Verificar calidad de la tabla consultas
    sql_check_consultas = """
    SELECT COUNT(*) FROM consultas 
    WHERE paciente_id IS NULL OR doctor_id IS NULL OR fecha_consulta IS NULL;
    """
    consultas_invalid = pg_hook.get_first(sql_check_consultas)[0]
    status_consultas = f"Error: {consultas_invalid} registros inválidos encontrados" if consultas_invalid > 0 else "Calidad de Dato Excelente"

    sql_update_consultas = f"""
    UPDATE consultas SET data_quality_status = '{status_consultas}';
    """
    pg_hook.run(sql_update_consultas, autocommit=True)

    print("✅ Calidad de datos actualizada en pacientes, doctores y consultas.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 13),
    'retries': 1,
}

dag = DAG(
    dag_id='data_quality_check',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

quality_check_task = PythonOperator(
    task_id='check_and_update_data_quality',
    python_callable=check_and_update_data_quality,
    dag=dag
)
