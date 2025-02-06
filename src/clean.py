from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, explode, from_json, year, month, round, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Initialiser Spark
spark = SparkSession.builder.appName("TransportAnalysis").getOrCreate()

# Charger les fichiers CSV
arrets_lignes_df = spark.read.option("header", True).option("sep", ";").csv("/home/vboxuser/PR6_Big_Data/data/arrets-lignes.csv")
disruptions_df = spark.read.option("header", True).csv("/home/vboxuser/PR6_Big_Data/data/disruptions.csv")
lines_df = spark.read.option("header", True).csv("/home/vboxuser/PR6_Big_Data/data/lines.csv")

# Nettoyage des arrêts (conversion des coordonnées)
arrets_lignes_df = arrets_lignes_df.withColumn("stop_lon", col("stop_lon").cast("double"))
arrets_lignes_df = arrets_lignes_df.withColumn("stop_lat", col("stop_lat").cast("double"))
arrets_lignes_df = arrets_lignes_df.dropna(subset=["stop_name", "stop_lon", "stop_lat"])

# Extraction des lignes impactées dans disruptions
impacted_schema = ArrayType(StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("name", StringType(), True)
]))
disruptions_df = disruptions_df.withColumn("impactedObjects", from_json(col("impactedObjects"), impacted_schema))
disruptions_exploded = disruptions_df.withColumn("line_id", explode(col("impactedObjects.id")))
disruptions_exploded = disruptions_exploded.drop("impactedObjects")

# Conversion des dates
disruptions_exploded = disruptions_exploded.withColumn("lastUpdate", to_timestamp(col("lastUpdate"), "yyyyMMdd'T'HHmmss"))

# Nombre de perturbations par ligne
perturbations_par_ligne = disruptions_exploded.groupBy("line_id").agg(count("id").alias("nombre_perturbations")).orderBy(col("nombre_pertubations").desc())

# Evolution des perturbations dans le temps
disruptions_time = disruptions_exploded.withColumn("year", year(col("lastUpdate"))) \
                                       .withColumn("month", month(col("lastUpdate")))
disruptions_by_time = disruptions_time.groupBy("year", "month").count()

# Nombre d'arrêts par ligne
arrets_par_ligne = arrets_lignes_df.groupBy("id").agg(count("stop_id").alias("nombre_arrets")).orderBy(col("nombre_arrets").desc())

# Regrouper les insights par ligne
final_df = lines_df.join(arrets_par_ligne, lines_df.id == arrets_par_ligne.id, "left") \
                   .join(perturbations_par_ligne, lines_df.id == perturbations_par_ligne.line_id, "left") \
                   .select(lines_df.id, lines_df.name, "nombre_arrets", "nombre_perturbations")

# Sauvegarde des résultats en format Parquet sur HDFS
final_df.write.mode("overwrite").csv("/home/vboxuser/PR6_Big_Data/data/final_data.csv")

# Afficher les résultats
final_df.show()


