package com.bigdatalogsproject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LogsAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Logs Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._  // <-- obligatoire pour $"col"

    val df = spark.read.option("header", "true").csv("data/logs.csv")

    val errorLogs = df.filter($"level" === "ERROR")
errorLogs.show()

// --- AJOUTER ICI ---
errorLogs.write.mode("overwrite").parquet("local/errors_parquet/")

val topErrors = errorLogs.groupBy("message").count().orderBy(desc("count"))
topErrors.show(10, false)

  }
}
