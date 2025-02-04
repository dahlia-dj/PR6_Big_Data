
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.utils import *

# Nettoyage des données
def df_cleaned(df:DataFrame):
    #df = extract_from_hdfs(datalake_path) #recuperation des données depuis hdfs
    df1= df.dropna(how="all")
    df_clean= df1.dropDuplicates() #suppression des valeurs nulles et les doublons
    return df_clean

if __name__=="__main__": # type: ignore

    from store import store_to_hdfs, extract_from_hdfs

    spark = SparkSession.builder.appName('csv_extraction').getOrCreate()
    data_static = extract_from_hdfs(spark,"hdfs://localhost:9000/user/ubuntu/datalake/data_csv/part-00000-aa692112-eef1-40bf-b296-055998ad0458-c000.csv") # type: ignore
    data_cleaned = df_cleaned(data_static)
    data_cleaned.show() 
    #store_to_hdfs(data_cleaned,"hdfs://localhost:9000/user/ubuntu/datastore/data_cleaned.parquet")




