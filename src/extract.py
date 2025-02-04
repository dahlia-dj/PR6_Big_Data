from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import * # type: ignore
import requests # type: ignore
import json
import pandas as pd # type: ignore
from store import store_to_hdfs


#extraction des données en csv
def extract_csv(spark:SparkSession, input_file:str):
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    #store_to_hdfs(df, output_file)
    return df

#extraction des données depuis l'api
def extract_from_api(api_url:str):
    response = requests.get(api_url)
    if response.status_code == 200:
        data  =response.json()
        return data
    else:
        raise Exception(f"Erreur API: {response.status_code}")
    
def convert_to_dataframe(spark,data):
    pdf = pd.DataFrame(data)  # Convertir JSON en DataFrame Pandas
    df = spark.createDataFrame(pdf)  # Convertir Pandas -> PySpark DataFrame
    return df


if __name__=="__main__": # type: ignore

    from store import store_to_hdfs

    spark = SparkSession.builder.appName('csv_extraction').getOrCreate()
    data_static = extract_csv(spark,"/home/ubuntu/Documents/PR6-BIGDATA/data/dioxide.csv") # type: ignore
    data_static.show() 
    store_to_hdfs(data_static,"hdfs://localhost:9000/user/ubuntu/datalake/data.parquet")   