from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import * # type: ignore
import requests # type: ignore
import json
from store import store_to_hdfs
from paths_server import datalake_path


#extraction des données en csv
def extract_csv(spark:SparkSession, input_file:str, output_file:str):
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    store_to_hdfs(df, output_file)


#extraction des données depuis l'api
def extract_from_api(api_url:str):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erreur API: {response.status_code}")
