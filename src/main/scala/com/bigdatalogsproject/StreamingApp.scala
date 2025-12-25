package com.bigdatalogsproject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Streaming App")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._  // <-- obligatoire

    val logsDF = spark.read.text("data/logs.csv")
  val logsSplitDF = logsDF
  .withColumn("timestamp", split($"value", ",").getItem(0))
  .withColumn("level", split($"value", ",").getItem(1))
  .withColumn("message", split($"value", ",").getItem(2))

logsSplitDF.show()

// --- AJOUTER ICI ---
val errorLogsStream = logsSplitDF.filter($"level" === "ERROR")

errorLogsStream.writeStream
  .format("parquet")
  .option("checkpointLocation", "local/checkpoint_errors/")
  .option("path", "local/errors_parquet_stream/")
  .outputMode("append")
  .start()
  .awaitTermination()

  }
}
