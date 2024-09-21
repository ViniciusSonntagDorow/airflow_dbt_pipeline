#from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
import gzip
import shutil
import os

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
            path = f"C:/Users/vinic/OneDrive/Documentos/python/airflow_dbt_pipeline/include/{ano}.gz"
            with open(path, "wb") as f:
                f.write(r.content)
        return path[0:71]

    @task()
    def unzip(path):
        for file in os.listdir(path):
            if file.endswith(".gz"):
                zip_url = os.path.join(path, file)
                output_file = zip_url[:-3] + ".csv"
                with gzip.open(zip_url, 'rb') as f_in:
                    with open(output_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)


# Instantiate the DAG
credito_rural()