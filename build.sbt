name := "BigDataLogsProject"
version := "0.1"
scalaVersion := "2.13.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "4.1.0",
  "org.apache.spark" %% "spark-sql"  % "4.1.0"
)
