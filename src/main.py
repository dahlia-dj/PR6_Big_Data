from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import * # type: ignore
from clean import data_cleaned
from extract import extract_csv
from store import store_to_hdfs
from paths_server import *

#creation de la session spark
spark = SparkSession.builder.appName("Big_Data").getOrCreate()

#Tous les liens vers les données
file_path="/home/ubuntu/Documents/PR6-BIGDATA/data/dioxide.csv"
#api_url=



data_static = extract_csv(spark,input_file=file_path, output_file=datalake_path+"/dioxide.csv")
#data_static.show()
#store_to_hdfs(data_static, datalake_path)
data_cleaned(data_static)
