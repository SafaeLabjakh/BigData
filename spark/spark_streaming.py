from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, current_timestamp
import sys
import os

print("=" * 50)
print("  Initialisation de Spark Streaming")
print("=" * 50)

try:
    # Créer la session Spark
    spark = SparkSession.builder \
        .appName("KafkaLogStreaming") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    # Définir le niveau de log
    spark.sparkContext.setLogLevel("WARN")

    print("\n✅ Session Spark créée avec succès")
    print(f"   Version Spark: {spark.version}")
    print(f"   Master: spark://spark-master:7077")

    # Lire depuis Kafka
    print("\n📡 Connexion à Kafka...")
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "app-logs") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("✅ Connexion à Kafka établie")
    print("   Topic: app-logs")
    print("   Bootstrap servers: kafka:9092")

    # Convertir en string et séparer les colonnes
    print("\n🔄 Configuration du traitement des logs...")
    logs_df = df.selectExpr("CAST(value AS STRING) as log") \
        .select(
            split(col("log"), ",").getItem(0).alias("timestamp"),
            split(col("log"), ",").getItem(1).alias("level"),
            split(col("log"), ",").getItem(2).alias("message")
        ) \
        .withColumn("processing_time", current_timestamp())

    # Afficher dans la console (test)
    print("\n📺 Démarrage de l'affichage console...")
    console_query = logs_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .trigger(processingTime='10 seconds') \
        .start()

    print("✅ Streaming console actif")

    # Écrire localement (parquet)
    local_path = os.path.join(os.getcwd(), "logs_parquet")
    checkpoint_path = os.path.join(os.getcwd(), "checkpoint_local")

    print(f"\n💾 Démarrage de l'écriture locale dans: {local_path}")
    local_query = logs_df.writeStream \
        .format("parquet") \
        .option("path", local_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("level") \
        .outputMode("append") \
        .trigger(processingTime='30 seconds') \
        .start()

    print("✅ Streaming local actif")
    print(f"   Path: {local_path}")
    print("   Partitionné par: level")

    print("\n" + "=" * 50)
    print("  🚀 SYSTÈME OPÉRATIONNEL")
    print("=" * 50)
    print("\n📊 En attente des données de Kafka...")
    print("🔄 Traitement toutes les 10 secondes (console)")
    print("💾 Sauvegarde toutes les 30 secondes (local)")
    print("\n🛑 Appuyez sur Ctrl+C pour arrêter proprement")
    print("=" * 50 + "\n")

    # Attendre la fin des queries
    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:
    print("\n" + "=" * 50)
    print("  ⏹️  ARRÊT DEMANDÉ")
    print("=" * 50)
    print("\n🔄 Arrêt des streams en cours...")
    
    if 'spark' in locals():
        for query in spark.streams.active:
            print(f"   Arrêt de: {query.name}")
            query.stop()
        spark.stop()
    
    print("✅ Arrêt réussi\n")
    sys.exit(0)

except Exception as e:
    print("\n" + "=" * 50)
    print("  ❌ ERREUR")
    print("=" * 50)
    print(f"\n{type(e).__name__}: {e}\n")
    
    import traceback
    traceback.print_exc()
    
    if 'spark' in locals():
        spark.stop()
    
    print("\n💡 Vérifiez que:")
    print("   - Kafka est accessible sur kafka:9092")
    print("   - Spark Master est accessible sur spark://spark-master:7077")
    print("   - Le topic 'app-logs' existe dans Kafka")
    
    sys.exit(1)
