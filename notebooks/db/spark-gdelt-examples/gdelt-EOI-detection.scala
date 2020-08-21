// Databricks notebook source
// MAGIC %md
// MAGIC # GDELT Events of Interest -- Detection
// MAGIC 
// MAGIC Johannes Graner, Albert Nilsson and Raazesh Sainudiin

// COMMAND ----------

import spark.implicits._
import io.delta.tables._
import com.aamend.spark.gdelt._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import java.sql.Date
import org.apache.spark.sql.functions._
import org.lamastex.spark.trendcalculus._


import org.apache.spark.sql.functions.to_date

import java.sql.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls s3a://osint-gdelt-reado/GDELT/del/bronze/normdailycountry

// COMMAND ----------

val gkg_v1 = spark.read.format("delta").load("s3a://osint-gdelt-reado/GDELT/del/bronze/v1/gkg").as[GKGEventV1]
val eve_v1 = spark.read.format("delta").load("s3a://osint-gdelt-reado/GDELT/del/bronze/v1/events").as[EventV1]

// COMMAND ----------

val gkg_v1_filt = gkg_v1.filter($"publishDate">"2013-04-01 00:00:00" && $"publishDate"<"2019-12-31 00:00:00")

val oil_gas_themeGKG = gkg_v1_filt.filter(c =>c.themes.contains("ENV_GAS") || c.themes.contains("ENV_OIL"))
                              .select(explode($"eventIds"))
                              .toDF("eventId")
                              .groupBy($"eventId")
                              .agg(count($"eventId"))
                              .toDF("eventId","count") 

// COMMAND ----------

val oil_gas_eventDF = eve_v1.toDF()
                          .join( oil_gas_themeGKG, "eventId")

 oil_gas_eventDF.write.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/oil_gas_event_v1")

// COMMAND ----------

val  oil_gas_eventDF = spark.read.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/oil_gas_event_v1")

// COMMAND ----------

def movingAverage(df:DataFrame,size:Int,avgOn:String):DataFrame = {
  val windowSpec = Window.partitionBy($"country").orderBy($"date").rowsBetween(-size/2, size/2)
return df.withColumn("coverage",avg(avgOn).over(windowSpec))
}
val oilEventTemp = oil_gas_eventDF.filter(length(col("eventGeo.countryCode")) > 0)
                                                     .groupBy(
                                                                col("eventGeo.countryCode").as("country"),
                                                                col("eventDay").as("date")
                                                            )
                                                     .agg(
                                                            sum(col("numArticles")).as("articles"),
                                                                  avg(col("goldstein")).as("goldstein")
                                                            )


val (mean_articles, std_articles) = oilEventTemp.select(mean("articles"), stddev("articles"))
  .as[(Double, Double)]
  .first()                                                     
  
val oilEventWeeklyCoverage = movingAverage(oilEventTemp.withColumn("normArticles", ($"articles"-mean_articles) /std_articles)                                                                                        ,7,"normArticles")      

oilEventWeeklyCoverage.write.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/oil_gas_cov_norm")

// COMMAND ----------

val oilEventWeeklyCoverage = spark.read.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/oil_gas_cov_norm/")

// COMMAND ----------

val oilEventWeeklyCoverageC = oilEventWeeklyCoverage.drop($"goldstein").drop($"normArticles").drop($"articles").toDF("country","tempDate","coverage")
val oilEventCoverageDF = oilEventWeeklyCoverageC.join(oil_gas_eventDF,oil_gas_eventDF("eventDay") === oilEventWeeklyCoverageC("tempDate") && oil_gas_eventDF("eventGeo.countryCode")
                       === oilEventWeeklyCoverageC("country"))
oilEventCoverageDF.write.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/oil_gas_eve_cov/")

// COMMAND ----------

val oil_gas_cov_norm = spark.read.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/oil_gas_cov_norm/")

// COMMAND ----------

oil_gas_cov_norm

// COMMAND ----------

// MAGIC %md
// MAGIC # Lets look at 2018 

// COMMAND ----------

display(oil_gas_cov_norm.filter($"date" >"2018-01-01" && $"date"<"2018-12-31").orderBy(desc("coverage")).limit(1000))

// COMMAND ----------

display(oil_gas_cov_norm.filter($"date" >"2018-01-01" && $"date"<"2018-12-31" && $"country" =!="US").orderBy(desc("coverage")).limit(1000))

// COMMAND ----------

//USA has so much more coverage than the rest 
display(oil_gas_cov_norm.filter($"date" >"2018-01-01" && $"date"<"2018-12-31" && $"country" ==="US").orderBy(desc("coverage")).limit(1000))

// COMMAND ----------

val normData = spark.read.format("delta").load("s3a://osint-gdelt-reado/GDELT/del/bronze/normdailycountry/").as[EventNormDailyByCountry]

// COMMAND ----------

// MAGIC %md 
// MAGIC # enrich data with trendCalculus

// COMMAND ----------

val oilData2018 = spark.read.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/johannes/streamable-trend-calculus/oilData2018").withColumn("ticker",lit("oil")).select($"ticker",$"x",$"y").as[TickerPoint]

val trend_oil_2018 = new TrendCalculus2(oilData2018,2,spark).nReversalsJoinedWithMaxRev(10)

trend_oil_2018.write.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/trend_oil_2018")

// COMMAND ----------

val oil_data_all = spark.read.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/johannes/streamable-trend-calculus/oilDataAll").withColumn("ticker",lit("oil")).select($"ticker",$"x",$"y").as[TickerPoint]
val trend_oil_all = new TrendCalculus2(oil_data_all,2,spark).nReversalsJoinedWithMaxRev(15)
trend_oil_all.write.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/trend_oil_all")

// COMMAND ----------

// MAGIC %md
// MAGIC # Python

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from plotly.offline import plot
// MAGIC from plotly.graph_objs import *
// MAGIC from datetime import *
// MAGIC from pyspark.sql import functions as F
// MAGIC import pyspark.sql.functions
// MAGIC from pyspark.sql.functions import col, avg

// COMMAND ----------

// MAGIC %python
// MAGIC trend_oil_all = spark.read.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/trend_oil_all")
// MAGIC 
// MAGIC oil_gas_cov_us_2015_2018 = spark.read.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/albert/texata/oil_gas_cov_norm/").select(F.col('date'),F.col('country'),F.col('coverage')).filter(F.col('country') == 'US').drop('country').filter(F.col('date')>'2015-01-01').filter(F.col('date')<'2018-12-31')
// MAGIC 
// MAGIC trend_oil_2015_2018 = trend_oil_all.filter(F.col('x')>'2015-01-01').filter(F.col('x')<'2018-12-31').orderBy(F.col('x'))
// MAGIC 
// MAGIC max_price = trend_oil_2015_2018.agg({'y': 'max'}).first()[0]
// MAGIC min_price = trend_oil_2015_2018.agg({'y': 'min'}).first()[0]
// MAGIC trend_oil_2015_2018_2 =trend_oil_2015_2018.withColumn('sy', (F.col('y')-min_price)/(max_price-min_price))
// MAGIC 
// MAGIC fullTS = trend_oil_2015_2018_2.filter("maxRev > 2").select("x","sy","maxRev").collect()
// MAGIC coverage =oil_gas_cov_us_2015_2018.collect()
// MAGIC 
// MAGIC TS = [row for row in fullTS]

// COMMAND ----------

// MAGIC %python
// MAGIC oil_gas_cov_us_2015_2018.count()

// COMMAND ----------

// MAGIC %md 
// MAGIC # 2015 - 2018

// COMMAND ----------

// MAGIC %python
// MAGIC numReversals = 15
// MAGIC startReversal = 7
// MAGIC 
// MAGIC allData = {'x': [row['x'] for row in TS], 'y': [row['sy'] for row in TS], 'maxRev': [row['maxRev'] for row in TS]}
// MAGIC allDataCov = {'x': [row['date'] for row in us_coverage], 'y': [row['coverage'] for row in us_coverage]}
// MAGIC 
// MAGIC temp2 = max(allDataCov['y'])-min(allDataCov['y'])
// MAGIC standardCoverage = list(map(lambda x: (x-min(allDataCov['y']))/temp2,allDataCov['y']))
// MAGIC 
// MAGIC revTS = [row for row in TS if row[2] >= startReversal]
// MAGIC colorList = ['rgba(' + str(tmp) + ',' + str(255-tmp) + ',' + str(255-tmp) + ',1)' for tmp in [int(i*255/(numReversals-startReversal+1)) for i in range(1,numReversals-startReversal+2)]]
// MAGIC 
// MAGIC def getRevTS(tsWithRevMax, revMax):
// MAGIC   x = [row[0] for row in tsWithRevMax if row[2] >= revMax]
// MAGIC   y = [row[1] for row in tsWithRevMax if row[2] >= revMax]
// MAGIC   return x,y,revMax
// MAGIC 
// MAGIC reducedData = [getRevTS(revTS, i) for i in range(startReversal, numReversals+1)]
// MAGIC 
// MAGIC markerPlots = [Scattergl(x=x, y=y, mode='markers', marker=dict(color=colorList[i-startReversal], size=i), name='Reversal ' + str(i)) for (x,y,i) in [getRevTS(revTS, i) for i in range(startReversal, numReversals+1)]]

// COMMAND ----------

// MAGIC %python
// MAGIC p = plot(
// MAGIC   [Scattergl(x=allData['x'], y=allData['y'], mode='lines', name='Oil Price'),Scattergl(x=allDataCov['x'], y=standardCoverage, mode='lines', name='Oil and gas coverage usa ')] + markerPlots 
// MAGIC   ,
// MAGIC   output_type='div'
// MAGIC )
// MAGIC displayHTML(p)