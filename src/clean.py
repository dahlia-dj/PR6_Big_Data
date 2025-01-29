from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from store import extract_from_hdfs,store_to_hdfs
from paths_server import *

# Nettoyage des données
def data_cleaned(df:DataFrame):
    df = extract_from_hdfs(datalake_path)
    df_clean = df.dropna(how:"all").dropDuplicates() #suppression des valeurs nulles et les doublons
    store_to_hdfs(df_clean,datastore_path)



