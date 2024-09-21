#from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
import zipfile

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["goiano analytics"],
)
def credito_rural():
    # Define tasks
    @task()
    def download_data(*anos):
        for ano in anos:
            url = f"https://www.bcb.gov.br/htms/sicor/DadosBrutos/SICOR_CONTRATOS_MUNICIPIO_{ano}.gz"
            r = requests.get(url)
            path = f"C:/Users/vinic/OneDrive/Documentos/python/airflow_dbt_pipeline/include/{ano}.rar"
            with open(path, "wb") as f:
                f.write(r.content)
        return path
    
    @task()


# Instantiate the DAG
credito_rural()
