import requests
import gzip
import shutil
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(DATABASE_URL)

def download_data(*anos):
    for ano in anos:
        url = f"https://www.bcb.gov.br/htms/sicor/DadosBrutos/SICOR_CONTRATOS_MUNICIPIO_{ano}.gz"
        r = requests.get(url)
        path = f"./{ano}.gz"
        with open(path, "wb") as f:
            f.write(r.content)

def unzip():
    for file in os.listdir("./"):
        if file.endswith(".gz"):
            zip_url = os.path.join(file)
            output_file = zip_url[:-3] + ".csv"
            with gzip.open(zip_url, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

def to_postgres():
    df_final = pd.DataFrame()
    for file in os.listdir("./"):
        csv_url = os.path.join(file)
        if file.endswith(".csv"):
            df = pd.read_csv(csv_url, sep="|", encoding="iso-8859-2")
            df_final = pd.concat([df_final, df])
    df.to_sql('creditorural', engine, if_exists='replace', index=True)

download_data(2024)

unzip()

to_postgres()
