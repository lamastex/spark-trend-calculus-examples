// Databricks notebook source
// MAGIC %md
// MAGIC # Streaming Trend Calculus with Maximum Necessary Reversals
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
// MAGIC This builds on the following library and its antecedents therein:
// MAGIC 
// MAGIC - [https://github.com/lamastex/spark-trend-calculus](https://github.com/lamastex/spark-trend-calculus)
// MAGIC 
// MAGIC 
// MAGIC ## This work was inspired by:
// MAGIC 
// MAGIC - Antoine Aamennd's [texata-2017](https://github.com/aamend/texata-r2-2017)
// MAGIC - Andrew Morgan's [Trend Calculus Library](https://github.com/ByteSumoLtd/TrendCalculus-lua)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC We use the spark-trend-calculus library and Spark structured streams over delta.io files to obtain a representation of the complete time series of trends with their k-th order reversal.
// MAGIC 
// MAGIC This representation is a sufficient statistic for a Markov model of trends that we show in the next notebook.

// COMMAND ----------

import java.sql.Timestamp
import io.delta.tables._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.lamastex.spark.trendcalculus._

// COMMAND ----------

// MAGIC %md
// MAGIC Input data in s3. The data contains oil price data from 2010 to last month and gold price data from 2009 to last month.

// COMMAND ----------

val rootPath = "s3a://xxx-yyy-zzz/canwrite/summerinterns2020/johannes/streamable-trend-calculus/"
val oilGoldPath = rootPath + "oilGoldDelta"

// COMMAND ----------

display(spark.read.format("delta").load(oilGoldPath).orderBy("x"))

// COMMAND ----------

// MAGIC %md
// MAGIC Reading the data from s3 as a Structured Stream to simulate streaming.

// COMMAND ----------

val input = spark
  .readStream
  .format("delta")
  .load(oilGoldPath)
  .as[TickerPoint]

// COMMAND ----------

// MAGIC %md
// MAGIC The Trend Calculus algorithm computes trend reversals of all orders in one pass. 
// MAGIC 
// MAGIC However, a point may be identified as a higher order reversal much later than it is originally identified as a first order reversal.
// MAGIC Due to the nature of streaming data, this cannot be corrected for during the stream, so aggregations to keep only the highest reversal order for each point must be done at a later stage, where the dataset is read as a static dataset.

// COMMAND ----------

val windowSize = 2
val sinkPath = rootPath + "stream/reversals"
val chkptPath = rootPath + "stream/checkpoint"

// COMMAND ----------

val stream = new TrendCalculus2(input, windowSize, spark)
  .reversals
  .select("tickerPoint.ticker", "tickerPoint.x", "tickerPoint.y", "reversal")
  .as[FlatReversal]
  .writeStream
  .format("delta")
  .option("path", sinkPath)
  .option("checkpointLocation", chkptPath)
  .trigger(Trigger.Once())
  .start

stream.processAllAvailable()

// COMMAND ----------

// MAGIC %md
// MAGIC The written delta table can be read as streams but for now we read it as a static dataset and keep only the highest order reversal at each point.

// COMMAND ----------

val revTable = DeltaTable.forPath(sinkPath).toDF.as[FlatReversal]
  .withColumn("reversalType", signum($"reversal"))
  .groupBy($"ticker", $"x", $"y", $"reversalType").agg((max(abs($"reversal")) * $"reversalType").cast("int") as "reversal")
  .drop($"reversalType").as[FlatReversal]

// COMMAND ----------

// MAGIC %md
// MAGIC The number of reversals decrease rapidly as the reversal order increases.

// COMMAND ----------

println("order\tcount")
revTable
  .groupBy(abs($"reversal") as "reversal").agg(count($"x") as "count")
  .orderBy("reversal").select("count").as[Long]
  .collect.scanRight(0L)(_ + _).dropRight(1).zipWithIndex
  .foreach(rev => println(s"${rev._2}:\t${rev._1}"))

// COMMAND ----------

// MAGIC %md
// MAGIC The reversal column contains the information of all orders of reversals.
// MAGIC 
// MAGIC `0` indicates that no reversal happens while a non-zero value indicates that this is a reversal point for that order and every lower order.
// MAGIC 
// MAGIC For example, row 33 contains the value `-4`, meaning that this point is trend reversal downwards for orders 1, 2, 3, and 4.

// COMMAND ----------

display(revTable.filter("ticker == 'BCOUSD'").orderBy("x"))
