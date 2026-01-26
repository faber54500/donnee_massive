from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, avg

# 1. On branche Spark sur le cluster et MongoDB
spark = SparkSession.builder \
    .appName("TP_Cosmetiques") \
    .master("spark://spark-master:7077") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/db_logs") \
    .getOrCreate()

# 2. On lit le fichier de logs dans Hadoop
logs = spark.read.text("hdfs://namenode:9000/web_server.log")

# 3. On crée un tableau propre (Indice 9 pour les chiffres)
tableau = logs.select(
    split(col("value"), " ").getItem(0).alias("ip"),
    split(col("value"), " ").getItem(6).alias("url"),
    split(col("value"), " ").getItem(9).cast("integer").alias("temps")
)

# 4. On prépare les 3 résultats
top_ips = tableau.groupBy("ip").count().orderBy("count", ascending=False).limit(10)

top_produits = tableau.filter(col("url").contains("/products/")) \
    .groupBy("url").count().orderBy("count", ascending=False).limit(10)

stats_cat = tableau.filter(col("url").contains("/products/")) \
    .withColumn("categorie", split(col("url"), "/").getItem(2)) \
    .groupBy("categorie").agg(avg("temps").alias("moyenne"))

# 5. Affichage console
top_ips.show()
top_produits.show()
stats_cat.show()

# 6. ENREGISTREMENT (Mode "overwrite" pour effacer les anciens doublons)
url_mongo = "mongodb://mongodb:27017/db_logs"

# On utilise "overwrite" au lieu de "append"
top_ips.write.format("mongo").mode("overwrite").option("uri", url_mongo).option("collection", "top_ips").save()
top_produits.write.format("mongo").mode("overwrite").option("uri", url_mongo).option("collection", "top_produits").save()
stats_cat.write.format("mongo").mode("overwrite").option("uri", url_mongo).option("collection", "stats_cat").save()

print("Succès ! La base de données a été mise à jour sans doublons.")