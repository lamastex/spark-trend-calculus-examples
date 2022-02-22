// Databricks notebook source
// MAGIC %md
// MAGIC # Markov Model for Trend Calculus
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
// MAGIC We use the dataset generated in the last notebook to build a simple, proof of concept Markov model for predicting trends.

// COMMAND ----------

import java.sql.Timestamp
import io.delta.tables._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.lamastex.spark.trendcalculus._
import scala.util.Random

// COMMAND ----------

dbutils.widgets.dropdown("m", "5", (1 to 10).map(_.toString).toSeq ++ Seq(15,20,25,30).map(_.toString) :+ "100")
dbutils.widgets.dropdown("n", "1", (1 to 3).map(_.toString).toSeq)
dbutils.widgets.dropdown("k", "max", (1 to 10).map(_.toString).toSeq :+ "max")
dbutils.widgets.dropdown("numTrainingSets", "10", (1 to 20).map( i => (i*5).toString).toSeq)

// COMMAND ----------

// MAGIC %md
// MAGIC Reading the joined dataset from the last notebook.
// MAGIC 
// MAGIC We train the model using both oil and gold data and predict trends in oil data. We show that this yields better results than just training on the oil data.

// COMMAND ----------

val maxRevPath = "s3a://osint-gdelt-reado/canwrite/summerinterns2020/johannes/streamable-trend-calculus/stream/reversals"
val maxRevDS = spark.read.format("delta").load(maxRevPath).as[FlatReversal]

// COMMAND ----------

// MAGIC %md
// MAGIC We want to predict what the trend of the next data point will be given the trend reversals we have observed.
// MAGIC 
// MAGIC For this, we use an m-th order Markov model. We look at the reversal state of the last `m` points and use this to predict the trends in the next `n` points. `k` is the maximum order of reversal that is considered when training the model.
// MAGIC 
// MAGIC `trainingRatio` is the ratio of the data used for training the model, the rest is used for testing.

// COMMAND ----------

val modelPath = "s3a://osint-gdelt-reado/canwrite/summerinterns2020/johannes/streamable-trend-calculus/estimators_new/"
val maxRevDSWithLagCountPath = modelPath + "maxRevDSWithLag_new"

val numPartitions = dbutils.widgets.get("numTrainingSets").toInt // 5
val partialModelPaths = (1 to numPartitions).map( i => modelPath + s"partialModel_new${i}" )
val fullModelPath = modelPath + "fullModel_new"

val m = dbutils.widgets.get("m").toInt // 5
val n = dbutils.widgets.get("n").toInt // 1
val k = dbutils.widgets.get("k") match { // 17
  case "max" => math.abs(maxRevDS.orderBy(abs($"reversal").desc).first.reversal) + 1
  case _ => dbutils.widgets.get("k").toInt
}
val trainingRatio = 0.7
type FinalModel = Map[Seq[Int], Map[Seq[Int], Double]]

// COMMAND ----------

def truncRev(k: Int)(rev: Int): Int = {
  if (math.abs(rev) > k) k*rev.signum else rev
}
val truncRevUDF = udf{ rev: Int => rev.signum }
def truncRevsUDF(k: Int) = udf{ revs: Seq[Int] => revs.map(truncRev(k)) }

def lagColumn(df: DataFrame, orderColumnName: String, lagKeyName: String, lagValueName: String, m: Int, n: Int): DataFrame = {
  val windowSpec = Window.partitionBy("ticker").orderBy(orderColumnName)
  val laggedKeyColNames = (1 to m).map( i => s"lagKey$i" ).toSeq
  val laggedValueColNames = (1 to n).map( i => s"lagValue$i" ).toSeq
  val dfWithLaggedKeyColumns = (n+1 to m+n)
    .foldLeft(df)( (df: DataFrame, i: Int) => df.withColumn(laggedKeyColNames(i-n-1), lag(lagKeyName, i-1, Int.MaxValue).over(windowSpec)) )
  val dfWithLaggedKeyValueColumns = (1 to n)
    .foldLeft(dfWithLaggedKeyColumns)( (df: DataFrame, i: Int) => df.withColumn(laggedValueColNames(i-1), lag(lagValueName, i-1, Int.MaxValue).over(windowSpec)) )
  
  dfWithLaggedKeyValueColumns
    .withColumn("lagKey", array(laggedKeyColNames.reverse.take(m).map(col(_)):_*))
    .withColumn("lagValue", array(laggedValueColNames.reverse.takeRight(n).map(col(_)):_*))
    .withColumn("lagKeyFirst", col(laggedKeyColNames.last))
    .filter($"lagKeyFirst" =!= Int.MaxValue)
    .drop("lagKeyFirst")
    .drop(laggedKeyColNames:_*)
    .drop(laggedValueColNames:_*)
}

// COMMAND ----------

// MAGIC %md
// MAGIC The trend at each point can be extracted from the trend reversals by taking the sum of all previous 1-st order trend reversals. This sum will always be either 0 (up trend) or -1 (down trend) and 0 is therefore mapped to 1 to get (1, -1) as (up, down).

// COMMAND ----------

val maxRevDSWithLag = lagColumn(
  maxRevDS
    .orderBy("x")
    .toDF
    .withColumn("truncRev", truncRevUDF($"reversal"))
    .withColumn("tmpTrend", sum("truncRev").over(Window.partitionBy("ticker").orderBy("x").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    .withColumn("trend", when($"tmpTrend" === 0, 1).otherwise(-1))
    .drop("truncRev", "tmpTrend"),
  "x", 
  "reversal",
  "trend",
  m, 
  n
)

// COMMAND ----------

// MAGIC %md
// MAGIC We now want to predict `lagValue` from `lagKey`.

// COMMAND ----------

display(maxRevDSWithLag)

// COMMAND ----------

// MAGIC %md
// MAGIC Cleaning up last run and writing model training input to delta tables.

// COMMAND ----------

dbutils.fs.rm(maxRevDSWithLagCountPath, recurse=true)

maxRevDSWithLag
  //.withColumn("lagValueTrunc", truncRevsUDF(1)($"lagValue"))
  .withColumn("count", lit(1L))
  .write
  .format("delta")
  .mode("overwrite")
  .save(maxRevDSWithLagCountPath)

// COMMAND ----------

partialModelPaths.map(dbutils.fs.rm(_, recurse=true))

// COMMAND ----------

val divUDF = udf{ (a: Long, b: Long) => a.toDouble/b }
val maxRevDSWithLagCount = spark.read.format("delta").load(maxRevDSWithLagCountPath)
val numberOfRows = maxRevDSWithLagCount.count

// COMMAND ----------

// MAGIC %md
// MAGIC The data is split into training and testing data. This is *not* done randomly as there is a dependence on previous data points. We don't want to train on data that is dependent on the testing data and therefore the training data consists on looking at the first (for example) 70% of the data and the last 30% is saved for testing. This also reflects how the model would be used since we can only train on data points that have already been observed.

// COMMAND ----------

val tickers = maxRevDSWithLagCount.select("ticker").distinct.as[String].collect.toSeq
val tickerDFs = tickers.map( ticker => maxRevDSWithLagCount.filter($"ticker" === ticker))
val trainingDF = tickerDFs.map( df => df.limit((df.count*trainingRatio).toInt) ).reduce( _.union(_) ).orderBy("x")
val trainingRows = trainingDF.count

val testingDF = maxRevDSWithLagCount.except(trainingDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Create `numTrainingSets` training set of increasing size to get snapshots of how a partially trained model looks like. The sizes are spaced logarithmically since the improvement in the model is fastest in the beginning.

// COMMAND ----------

val rowsInPartitions = (1 to numPartitions).map{ i: Int => (math.exp(math.log(trainingRows)*i/numPartitions)).toInt }//.scanLeft(0.0)(_-_)

// COMMAND ----------

// MAGIC %md
// MAGIC Model is trained by counting how many times each (`lagKey`, `lagValue`) pair is observed and dividing by how many times `lagKey` is observed to get an estimation of the transition probabilities.

// COMMAND ----------

val keyValueCountPartialDFs = rowsInPartitions.map(
  trainingDF
    .limit(_)
    .withColumn("keyValueObs", sum("count").over(Window.partitionBy($"lagKey", $"lagValue")))
    .withColumn("totalKeyObs", sum("count").over(Window.partitionBy($"lagKey")))
    .drop("count")
)

// COMMAND ----------

display(keyValueCountPartialDFs.last.orderBy($"keyValueObs".desc))

// COMMAND ----------

keyValueCountPartialDFs
  .map( df =>
    df
      .withColumn("probability", divUDF($"keyValueObs", $"totalKeyObs"))
      .drop("keyValueObs", "totalKeyObs")
  )
  .zip(partialModelPaths).foreach{ case (df: DataFrame, path: String) =>
    df.write.mode("overwrite").format("delta").save(path)  
  }

// COMMAND ----------

val probDFs = partialModelPaths.map(spark.read.format("delta").load(_))

// COMMAND ----------

display(probDFs.last.orderBy("probability"))

// COMMAND ----------

// MAGIC %md
// MAGIC The prediction is given by taking 
// MAGIC 
// MAGIC $$ V \in argmax(P_K(V)) $$ 
// MAGIC 
// MAGIC where *P_K(V)* is the probability that *V* is the next trend when the last `m` points have had reversals *K*. If there are more than one elements in argmax, an element is chosen uniformly at random.

// COMMAND ----------

val aggWindow = Window.partitionBy("lagKey").orderBy('probability desc)
val testedDFs = probDFs
  .map { df =>
     val predictionDF = df
      .select("lagKey", "lagValue", "probability")
      .distinct
      .withColumn("rank", rank().over(aggWindow))
      .filter("rank == 1")
      .groupBy("lagKey")
      .agg(collect_list("lagValue"))
    
    testingDF
      .filter($"ticker" === "BCOUSD")
      .join(predictionDF, Seq("lagKey"), "left")
      .withColumnRenamed("collect_list(lagValue)", "test")
  }

// COMMAND ----------

// MAGIC %md
// MAGIC We use a binary loss function that indicates if the prediction was correct or not.

// COMMAND ----------

val getRandomUDF = udf( (arr: Seq[Seq[Int]]) => {
  val safeArr = Option(arr).getOrElse(Seq[Seq[Int]]())
  if (safeArr.isEmpty) Seq[Int]() else safeArr(Random.nextInt(safeArr.size))
} )

val lossUDF = udf{ (value: Seq[Int], pred: Seq[Int]) =>
  if (value == pred) 0 else 1
}

// COMMAND ----------

val lossDFs = testedDFs.map(_.withColumn("prediction", getRandomUDF($"test")).withColumn("loss", lossUDF($"lagValue", $"prediction")))

// COMMAND ----------

display(lossDFs.last)

// COMMAND ----------

val testLength = testingDF.count
val oilTestDF = testingDF.filter($"ticker" === "BCOUSD")
val oilTestLength = oilTestDF.count

// COMMAND ----------

// MAGIC %md
// MAGIC We find the mean loss for each training dataset of increasing size. As one can see, the loss decreases as more data is supplied.
// MAGIC 
// MAGIC Further, training on both oil and gold data yields a better result than just oil, suggesting that trends behave similarly in the two commodities.

// COMMAND ----------

val losses = lossDFs.map( _.agg(sum("loss")).select($"sum(loss)".as("totalLoss")).collect.head.getLong(0).toDouble/oilTestLength )
// Data up to 2019
// k=max, m=2 ,n=1: (0.5489615950080244, 0.4014721726541194, 0.3660973816818738, 0.36497087640146797, 0.36485163238050344)
// k=max, m=10,n=1: (0.9812730246821787, 0.8523390131056561, 0.5819573469954631, 0.3552177121357085, 0.2807503135425113)
// k=max,m=100,n=1: (1.0, 1.0, 1.0, 1.0, 1.0)
// k=5  , m=10,n=1: (0.9812730246821787, 0.8522267831239777, 0.5820597568537447, 0.3550507700379618, 0.2806352778112909)
// k=1  , m=10,n=1: (0.9812730246821787, 0.852200128503329, 0.5821242890932098, 0.3553383593660128, 0.2806549180580846)

// k=max, m=10,n=2: (0.9879380657977248, 0.9049354606556204, 0.7243136776273427, 0.5472986906951395, 0.4783823708897465)

// k=max(17), m=10,n=1, 10 training sets: (0.9977203284971564, 0.9812730246821787, 0.9455375956409875, 0.8523810993487855, 0.7183644724769999, 0.5819320952495854, 0.44607349380350214, 0.35513915114853356, 0.3029480010437388, 0.280629666312207)

// Data up to last month
// k=max(18), m=10,n=1, 10 training sets: (0.9973104051296998, 0.9843882250072865, 0.9510789857184494, 0.8574351501020111, 0.7515744680851064, 0.6360547945205479, 0.5099656076945497, 0.4249921305741766, 0.3735668901194987, 0.34609501603031184)
// Could the difference be due to Corona?

// Trained on both oil and gold. testing on oil as previously.
// k=max(18), m=10,n=1, 10 training sets: (0.9999778490236083, 0.9980728650539201, 0.9527158262897114, 0.8921317400174876, 0.7559988341591373, 0.5820915185077237, 0.46675488195861264, 0.3923101136694841, 0.3574876129408336, 0.3355837948120082)

// With Trend Calculus 3.0, k=max, m=10, n=1, 10 training sets, gold + oil, data up to July 2020:
// (0.9999941707209302, 0.9985007094232627, 0.9521334578467343, 0.886164672470297, 0.7528152503267311, 0.5654190843601609, 0.42830045037010095, 0.3455165265890906, 0.30489694417532603, 0.28647991988238847)

// COMMAND ----------

val trainingSizes = probDFs.map(_.count)
val lossesDS = sc
  .parallelize(losses.zip(trainingSizes))
  .toDF("loss", "size")
  .withColumn("training", lit("Oil and Gold"))
  .as[(Double,Long,String)]

// COMMAND ----------

display(lossesDS)

// COMMAND ----------

// MAGIC %md
// MAGIC We collect the models in order to calculate the total variation distance between them.

// COMMAND ----------

val partialModels = probDFs.map{df => 
  val tmpMap = df.select("lagKey", "lagValue", "probability").distinct.collect.map{ r => 
    (r.getAs[Seq[Int]](0), r.getAs[Seq[Int]](1), r.getDouble(2))
  }.groupBy(_._1).mapValues(_.map(tup => Map(tup._2 -> tup._3)).flatten.toMap)
  
  tmpMap: FinalModel
}

// COMMAND ----------

def totalVarDist(m1: FinalModel, m2: FinalModel): Map[Seq[Int], Double] = {
  val allKeys = m1.keys.toSet.union(m2.keys.toSet)
  val sharedKeys = m1.keys.toSet.intersect(m2.keys.toSet)
  val totalVarDists = allKeys.toSeq.map{ key =>
    if (!sharedKeys.contains(key)) {
      1.0
    } else {
      val val1 = m1.getOrElse(key, Map())
      val val2 = m2.getOrElse(key, Map())
      val allValKeys = val1.keys.toSet.union(val2.keys.toSet)
      allValKeys.map( valKey => 0.5*math.abs(val1.getOrElse(valKey, 0.0) - val2.getOrElse(valKey, 0.0)) ).sum
    }
  }
  allKeys.zip(totalVarDists).toMap
}

// COMMAND ----------

val totalVariationDistances = partialModels.map( m1 => partialModels.map( m2 => totalVarDist(m1,m2) ) )

// COMMAND ----------

def aggToMatrix(totalVarDists: Seq[Seq[Map[Seq[Int],Double]]], aggFunc: Seq[Double] => Double): Seq[Seq[Double]] = {
  totalVariationDistances.map(_.map( t => aggFunc(t.values.toSeq)))
}

def printMatrix(mat: Seq[Seq[Double]]): Unit = {
  mat.map( s => { s.map( a => print(f"$a%2.3f ") ); println() } )
}

// COMMAND ----------

val maxDists = aggToMatrix(totalVariationDistances, s => s.max)
val minDists = aggToMatrix(totalVariationDistances, s => s.min)
val meanDists = aggToMatrix(totalVariationDistances, s => s.sum/s.size)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Each model is a mapping `{key: {value: probability}}` where `key` is a sequence of reversals and non-reversals of length `m`, `value` is a sequence of trends of length `n` and `probability` is the estimated probability that `value` is observed directly after `key`.
// MAGIC 
// MAGIC Hence, for any two models A and B, we can calculate the total variation distance between the mappings `{value: probability}` for a given key in the union of the keys for A and B. If a key is not present in one of the models, the total variation distance is 1 for that key.
// MAGIC 
// MAGIC In the matrix below, the (i,j)-th position is the arithmetic mean of the total variation distances for all keys in the union of the keysets. The matrix is symmetric with the smallest model in the top row and leftmost column and the largest model in the bottom row and rightmost column.
// MAGIC 
// MAGIC If there are three models labeled M_1, M_2, M_3 and V_i_j is the arithmetic mean described above, the matrix is
// MAGIC 
// MAGIC $$ \begin{matrix} V_{1,1} & V_{1,2} & V_{1,3} \end{matrix} $$
// MAGIC $$ \begin{matrix} V_{2,1} & V_{2,2} & V_{2,3} \end{matrix} $$
// MAGIC $$ \begin{matrix} V_{3,1} & V_{3,2} & V_{3,3} \end{matrix} $$
// MAGIC 
// MAGIC As one can see, the models differ a lot from each other, suggesting that the estimate can still be improved given more data.

// COMMAND ----------

printMatrix(meanDists)
