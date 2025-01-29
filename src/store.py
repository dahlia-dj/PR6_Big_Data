from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#stockage des données avec HDFS
def store_to_hdfs(df:DataFrame,output_path:str):
    df.write.mode("overwrite").csv(output_path)
    
#Récuperation des données depuis HDFS
def extract_from_hdfs(spark:SparkSession,input_path:str):
    df = spark.read.csv(input_path,header=True,inferSchema=True)
    return df