// Databricks notebook source
// MAGIC %md
// MAGIC # Historical Yahoo! Finance data
// MAGIC 
// MAGIC Johannes Graner, Albert Nilsson and Raazesh Sainudiin
// MAGIC 
// MAGIC 2020, Uppsala, Sweden
// MAGIC 
// MAGIC This project was supported by Combient Mix AB through summer internships at:
// MAGIC 
// MAGIC Combient Competence Centre for Data Engineering Sciences, 
// MAGIC Department of Mathematics, 
// MAGIC Uppsala University, Uppsala, Sweden
// MAGIC 
// MAGIC ## Resources
// MAGIC 
// MAGIC This builds on the following repository:
// MAGIC 
// MAGIC - https://github.com/philipperemy/FX-1-Minute-Data

// COMMAND ----------

// MAGIC %md
// MAGIC The [Trend Calculus library](https://github.com/lamastex/spark-trend-calculus) is needed for case classes and parsers for the data.

// COMMAND ----------

import org.lamastex.spark.trendcalculus._

// COMMAND ----------

val filePathRoot = "s3a://osint-gdelt-reado/findata/com/histdata/free/FX-1-Minute-Data/"

// COMMAND ----------

display(dbutils.fs.ls(filePathRoot))

// COMMAND ----------

val oilPath = filePathRoot + "bcousd/*.csv.gz"
val oilDS = spark.read.fx1m(oilPath).orderBy($"time")

// COMMAND ----------

display(oilDS)