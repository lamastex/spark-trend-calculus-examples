// Databricks notebook source
import java.sql.Timestamp
import io.delta.tables._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.lamastex.spark.trendcalculus._

// COMMAND ----------

val rootPath = "s3a://osint-gdelt-reado/canwrite/summerinterns2020/johannes/streamable-trend-calculus/"
val oil2018Path = rootPath + "oilData2018"
val oilAllPath = rootPath + "oilDataAll"
val gold2010Path = rootPath + "goldData2010"
val oilGoldPath = rootPath + "oilGoldDelta"

// COMMAND ----------

display(spark.read.format("delta").load(oilGoldPath).orderBy("x"))

// COMMAND ----------

//val timePointSchema = (new StructType).add("x", "timestamp").add("y", "double")
//val tickerPointSchema = (new StructType).add("ticker", "string").add("x", "timestamp").add("y", "double")

// COMMAND ----------

val numReversals = 15
val windowSize = 2
val inputPath = oilGoldPath

// COMMAND ----------

val input = spark
  .readStream
  .format("delta")
  .load(oilGoldPath)
  .as[TickerPoint]

// COMMAND ----------

case class flatReversal(
  ticker: String,
  x: Timestamp,
  y: Double,
  reversal: Int
)

// COMMAND ----------

//val flatRevSchema = new StructType().add("ticker", "string").add("x", "timestamp").add("y", "double").add("reversal", "int")

// COMMAND ----------

var i = 1
var prevSinkPath = ""
var sinkPath = rootPath + "multiSinks/reversal" + (i)
var chkptPath = rootPath + "multiSinks/checkpoint/" + (i)

var stream = new TrendCalculus2(input, windowSize, spark)
  .reversals
  .select("tickerPoint.ticker", "tickerPoint.x", "tickerPoint.y", "reversal")
  .as[flatReversal]
  .writeStream
  .format("delta")
  .option("path", sinkPath)
  .option("checkpointLocation", chkptPath)
  .trigger(Trigger.Once())
  .start

stream.processAllAvailable

i += 1

var lastReversalSeries = spark.emptyDataset[TickerPoint]
while (!spark.read.format("delta").load(sinkPath).isEmpty) {
  prevSinkPath = rootPath + "multiSinks/reversal" + (i-1)
  sinkPath = rootPath + "multiSinks/reversal" + (i)
  chkptPath = rootPath + "multiSinks/checkpoint/" + (i)
  try {
    lastReversalSeries = spark
      .readStream
      .format("delta")
      .load(prevSinkPath)
      .drop("reversal")
      .as[TickerPoint]
  } catch {
    case e: Exception => {
      println("i: " + i + ". prevSinkPath: " + prevSinkPath)
      throw e
    }
  }

  stream = new TrendCalculus2(lastReversalSeries, windowSize, spark)
    .reversals
    .select("tickerPoint.ticker", "tickerPoint.x", "tickerPoint.y", "reversal")
    .as[flatReversal]
    .map( rev => rev.copy(reversal=i*rev.reversal))
    .writeStream
    .format("delta")
    .option("path", sinkPath)
    .option("checkpointLocation", chkptPath)
    .partitionBy("ticker")
    .trigger(Trigger.Once())
    .start
  
  stream.processAllAvailable()
  i += 1
}

// COMMAND ----------

// DELETES THE SINKS
//dbutils.fs.rm(rootPath + "multiSinks", recurse=true)

// COMMAND ----------

val i = dbutils.fs.ls(rootPath + "multiSinks").length - 1

// COMMAND ----------

val sinkPaths = (1 to i-1).map(rootPath + "multiSinks/reversal" + _)
val maxRevPath = rootPath + "maxRev"
val revTables = sinkPaths.map(DeltaTable.forPath(_).toDF.as[flatReversal])
val oilGoldTable = DeltaTable.forPath(oilGoldPath).toDF.as[TickerPoint]

// COMMAND ----------

revTables.map(_.cache.count)

// COMMAND ----------

def maxByAbs(a: Int, b: Int): Int = {
  Seq(a,b).maxBy(math.abs)
}

val maxByAbsUDF = udf((a: Int, b: Int) => maxByAbs(a,b))

// COMMAND ----------

val maxRevDS = revTables.foldLeft(oilGoldTable.toDF.withColumn("reversal", lit(0)).as[flatReversal]){ (acc: Dataset[flatReversal], ds: Dataset[flatReversal]) => 
  acc
    .toDF
    .withColumnRenamed("reversal", "oldMaxRev")
    .join(ds.select($"ticker" as "tmpt", $"x" as "tmpx", $"reversal" as "newRev"), $"ticker" === $"tmpt" && $"x" === $"tmpx", "left")
    .drop("tmpt", "tmpx")
    .na.fill(0,Seq("newRev"))
    .withColumn("reversal", maxByAbsUDF($"oldMaxRev", $"newRev"))
    .select("ticker", "x", "y", "reversal")
    .as[flatReversal]    
}

// COMMAND ----------

maxRevDS.write.format("delta").partitionBy("ticker").save(maxRevPath)

// COMMAND ----------

display(DeltaTable.forPath(maxRevPath).toDF.as[flatReversal].filter("ticker == 'BCOUSD'").orderBy("x"))