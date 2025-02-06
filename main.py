# from pyspark.sql import SparkSession # type: ignore
# from pyspark.sql.functions import * # type: ignore
# from extract import extract_csv # type: ignore

# #Tous les liens vers les données
# file_path="/home/ubuntu/Documents/PR6-BIGDATA/data/dioxide.csv"
# #api_url=https://api.weatherapi.com/v1/current.json?key=AIzaSyDOu-thnvC8nSmuxQytjKajRFpvQ76ZQug&q=France&aqi=no
# datastore_path ="hdfs://localhost:9000/user/ubuntu/datastore"
# datalake_path ="hdfs://localhost:9000/user/ubuntu/datalake/data_csv"


# #creation de la session spark
# spark = SparkSession.builder.appName("Big_Data").getOrCreate()

# def extract_csv(spark:SparkSession, input_file:str):
#     df = spark.read.csv(input_file, header=True, inferSchema=True)
#     #store_to_hdfs(df, output_file)
#     return df

# if_name__=="__main__": # type: ignore

# data_static = extract_csv(spark,file_path)
# data_static.show()
# #data_static = extract_from_hdfs(spark,datalake_path+"/dioxide.csv/part-00000-9f039a3e-c371-4c65-bd19-84ff9956dbd6-c000.csv")
# #data_static.show()
# #data_clean = data_cleaned(data_static)
# #store_to_hdfs(data_cleaned, datastore_path+"/dioxide_new.csv"

