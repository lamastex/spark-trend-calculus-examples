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

//val i = dbutils.fs.ls(rootPath + "multiSinks").length - 1

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

// COMMAND ----------

val maxRevDS = spark.read.format("delta").load(maxRevPath).as[flatReversal]

// COMMAND ----------

val testMap = Map[String, Int](("hej", 1))
testMap.updated("då", testMap.getOrElse("då", 0)*2)

// COMMAND ----------

val m = 3
val n = 2
type InTrainingModel = Map[Seq[Int], (Map[Seq[Int], Long], Long)]
type FinalModel = Map[Seq[Int], Map[Seq[Int], Double]]
case class MCState(
  model: InTrainingModel,
  buffer: Seq[flatReversal]
)
case class ModelOutput(
  ticker: String,
  x: Timestamp,
  y: Double,
  reversal: Int,
  model: FinalModel
)

// COMMAND ----------

val testMap = Map("0" -> 0, "1" -> 1)

// COMMAND ----------

testMap.updated("0", testMap.getOrElse("0", 1))

// COMMAND ----------

val testSeq = Seq(1,2,3)
//testSeq.scan("0")( (acc: String, i: Int) => acc+i).drop(1)

// COMMAND ----------

testSeq.sliding(2,1).map(ls => ls.toSeq).next

// COMMAND ----------

val emptyFlatReversal = flatReversal("", new Timestamp(0L), 0.0, 0)

// COMMAND ----------

def trainModel(m: Int, n: Int)(ticker: String, input: Iterator[flatReversal], state: GroupState[MCState]): Iterator[ModelOutput] = {
  
  val oldState = state.getOption.getOrElse(MCState(Map[Seq[Int], (Map[Seq[Int], Long], Long)](), Seq[flatReversal]()))
  val values = (oldState.buffer ++ input.toSeq).sortBy(_.x.getTime)
  
  def updateModel(model: InTrainingModel, key: Seq[flatReversal], value: Seq[flatReversal]): InTrainingModel = {
    // The map that `key` points to
    val revKey = key.map(_.reversal)
    val revValue = value.map(_.reversal)
    val innerMap = model.getOrElse(revKey, (Map[Seq[Int], Long](), 0L))
    
    model.updated(
      revKey,
      (
        innerMap._1.updated(revValue, innerMap._1.getOrElse(revValue, 0L) + 1L), // Update innerMap with new observation
        innerMap._2 + 1L // Update total number of observations of key
      )
    )
  }
  
  val oldModel = oldState.model
  val windowedValues: Iterator[Seq[flatReversal]] = values.sliding(m+n)
  
  def foldFunc(acc: Seq[Tuple2[flatReversal, InTrainingModel]], revSeq: Seq[flatReversal]): Seq[Tuple2[flatReversal, InTrainingModel]] = {
    acc :+ Tuple2(revSeq.last, updateModel(acc.last._2, revSeq.take(m), revSeq.takeRight(n)))
  }
  
  val trainedSeq = windowedValues.foldLeft(Seq(Tuple2(emptyFlatReversal, oldModel)))(foldFunc).drop(1)
  
  val latestModel = trainedSeq.last._2
  val newBuffer = values.takeRight(m+n-1)
  state.update(MCState(latestModel, newBuffer))
  
  val outputSeq = trainedSeq.map{ case(rev: flatReversal, trainingModel: InTrainingModel) =>
    ModelOutput(rev.ticker, rev.x, rev.y, rev.reversal, trainingModel.mapValues{ case(innerMap: Map[Seq[Int], Long], count: Long) =>
      innerMap.mapValues(_.toDouble/count)
    } )
  }
  
  outputSeq.toIterator
  
}

// COMMAND ----------

Seq(1,2,3).sliding(4).toList

// COMMAND ----------

Window.rowsBetween(Window.unboundedPreceding, 0)

// COMMAND ----------

def lagColumn(df: DataFrame, orderColumnName: String, lagColumnName: String, rowsToLag: Int): DataFrame = {
  val windowSpec = Window.orderBy(orderColumnName)
  val laggedColNames = (1 to rowsToLag).map( i => s"lag$i" ).toSeq
  val dfWithLaggedColumns = (1 to rowsToLag).foldLeft(df)( (df: DataFrame, i: Int) => df.withColumn(laggedColNames(i-1), lag(lagColumnName, i-1, Int.MaxValue).over(windowSpec)) )
  dfWithLaggedColumns.withColumn("lagArray", array(laggedColNames.reverse.map(col(_)):_*)).drop(laggedColNames:_*)
}

// COMMAND ----------



// COMMAND ----------

def arrayToModel(array: scala.collection.mutable.WrappedArray[Int], m: Int, n: Int): InTrainingModel = {
  val key = array.take(m).toSeq
  
  if (key.contains(Int.MaxValue)) {
    return Map()
  }
  
  val value = array.takeRight(n).toSeq
  
  
  
  val innerMap = Map(value -> 1L)
  Map(key -> (innerMap, 1L))
}

def arrayToModelUDF(m: Int, n: Int) = udf{ array: scala.collection.mutable.WrappedArray[Int] => arrayToModel(array,m,n) }

// COMMAND ----------

display(lagColumn(maxRevDS.toDF.filter("ticker == 'BCOUSD'"), "x", "reversal", 5).withColumn("trainingModel", arrayToModelUDF(m,n)($"lagArray")).drop("lagArray"))

// COMMAND ----------

case class FlatRevWithModel(ticker: String, x: Timestamp, y: Double, reversal: Int, model: InTrainingModel)

// COMMAND ----------

val maxRevDSWithModel = lagColumn(maxRevDS.toDF.filter("ticker == 'BCOUSD'"), "x", "reversal", 5).withColumn("model", arrayToModelUDF(m,n)($"lagArray")).drop("lagArray").as[FlatRevWithModel]

// COMMAND ----------

val test = maxRevDSWithModel.take(10).takeRight(2).map(_.model)

// COMMAND ----------

val m1 = test(0)
val m2 = test(1)

// COMMAND ----------

(m1.toSeq ++ m2.toSeq).groupBy(_._1).mapValues{ arr: Array[(Map[Seq[Int],Long],Long)] => 
      val totalCount = arr.map(_._2).sum
      val updatedInnerMap = arr.map(_._1.toSeq).groupBy(_._1).mapValues(_._2.sum).toMap
      (updatedInnerMap, totalCount)
    }

// COMMAND ----------

val arrTest = (m1.toSeq ++ m2.toSeq).groupBy(_._1).mapValues(_.map(_._2)).values.head.map(_._1.toSeq).map(_.groupBy(_._1).mapValues(_.map(_._2).sum))

// COMMAND ----------

(m1.toSeq ++ m2.toSeq)
    .groupBy(_._1)
    .mapValues(_.map(_._2)).mapValues{ arr: Seq[(Map[Seq[Int],Long],Long)] => 
      val totalCount = arr.map(_._2).sum
      val updatedInnerMaps = arr.map(_._1.toSeq).foldLeft(Seq[(Seq[Int],Long)]())(_ ++ _).groupBy(_._1).mapValues(_.map(_._2).sum)
      (updatedInnerMaps, totalCount)
    }: InTrainingModel

// COMMAND ----------

Seq(m1,m2).map(_.toSeq).reduce(_ ++ _)

// COMMAND ----------

def mergeModels(inputModels: InTrainingModel*): InTrainingModel = {
  inputModels.map(_.toSeq).reduce(_ ++ _)
    .groupBy(_._1)
    .mapValues(_.map(_._2)).mapValues{ arr: Seq[(Map[Seq[Int],Long],Long)] => 
      val totalCount = arr.map(_._2).sum
      val updatedInnerMaps = arr.map(_._1.toSeq).foldLeft(Seq[(Seq[Int],Long)]())(_ ++ _).groupBy(_._1).mapValues(_.map(_._2).sum)
      (updatedInnerMaps, totalCount)
    }
}

// COMMAND ----------

Seq(1,2,3).scanLeft(0)(_+_)

// COMMAND ----------

def coalesceModels(ticker: String, input: Iterator[FlatRevWithModel], state: GroupState[InTrainingModel]): Iterator[ModelOutput] = {
  val values = input.toSeq
  val oldModel = state.getOption.getOrElse(Map())
  val updatedModels = Seq(mergeModels(values.map(_.model):_*))//values.map(_.model).scanLeft(oldModel)( (last: InTrainingModel, curr: InTrainingModel) => mergeModels(last, curr) ).drop(1)
  val newModel = updatedModels.last
  state.update(newModel)
  
  val finalModels = updatedModels.map(_.mapValues{ case (innerMap: Map[Seq[Int],Long], count: Long) => innerMap.mapValues(_.toDouble/count) })
  
  val outputSeq = (0 to values.length-1).map{ i => val flatRev = values(i); ModelOutput(flatRev.ticker, flatRev.x, flatRev.y, flatRev.reversal, finalModels(i))}
  outputSeq.toIterator
}

// COMMAND ----------

val modelTest = maxRevDSWithModel
  .orderBy("x")
  .limit(20000)
  .groupByKey(_.ticker)
  .flatMapGroupsWithState(
    outputMode = OutputMode.Append,
    timeoutConf = GroupStateTimeout.NoTimeout
  )(coalesceModels)

// COMMAND ----------

modelTest.isStreaming

// COMMAND ----------

modelTest.count