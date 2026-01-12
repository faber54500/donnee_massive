from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, count

# 1. Initialisation de la Session Spark
# Utilisation de .getOrCreate() pour la stabilité de la session
spark = SparkSession.builder \
    .appName("Analyse Logs Web Cosmétiques") \
    .master("spark://spark-master:7077") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/db_logs") \
    .getOrCreate() 

# 2. Chargement du fichier depuis HDFS
# On pointe vers le fichier web_server.log stocké sur le NameNode (port 9000)
df_brut = spark.read.text("hdfs://namenode:9000/web_server.log")

# 3. Extraction des colonnes (Format Standard des Logs)
df_split = df_brut.select(
    split(col("value"), " ").getItem(0).alias("ip"),           # Adresse IP
    split(col("value"), " ").getItem(8).alias("code_http"),    # Code Status HTTP
    split(col("value"), " ").getItem(6).alias("url")           # URL demandée
)

# --- ANALYSE 1 : Top 10 des adresses IP les plus actives ---
top_ips = df_split.groupBy("ip").agg(count("*").alias("nb_requetes")) \
    .orderBy(col("nb_requetes").desc()).limit(10)

# --- ANALYSE 2 : Répartition des codes HTTP (200, 404, etc.) ---
stats_http = df_split.groupBy("code_http").agg(count("*").alias("total"))

# --- ANALYSE 3 : Top des produits consultés ---
# On filtre les requêtes vers les pages produits cosmétiques
top_produits = df_split.filter(col("url").contains("/products/")) \
    .groupBy("url").agg(count("*").alias("vues")) \
    .orderBy(col("vues").desc()).limit(10)

# 4. Affichage des résultats dans la console
print("=== TOP 10 IP ===")
top_ips.show()
print("=== CODES HTTP ===")
stats_http.show()
print("=== TOP PRODUITS ===")
top_produits.show()

# 5. Sauvegarde des résultats dans MongoDB
# IMPORTANT : On utilise le format "mongo" pour le connecteur Spark-MongoDB
try:
    top_ips.write.format("mongo").mode("append").option("collection", "top_ips").save()
    stats_http.write.format("mongo").mode("append").option("collection", "stats_http").save()
    top_produits.write.format("mongo").mode("append").option("collection", "top_produits").save()
    print("Sauvegarde dans MongoDB effectuée avec succès.")
except Exception as e:
    print(f"Erreur lors de la sauvegarde MongoDB : {e}")