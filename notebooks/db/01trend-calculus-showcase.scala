// Databricks notebook source
// MAGIC %md
// MAGIC # Finding trends in Oil Price data.

// COMMAND ----------

// MAGIC %md
// MAGIC When dealing with time series, it can be difficult to find a good way to find and analyze trends in the data. 

// COMMAND ----------

import org.lamastex.spark.trendcalculus._
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.Timestamp

// COMMAND ----------

// MAGIC %md
// MAGIC The input to the algorithm is data in the format (ticker, time, value). In this example, ticker is `"BCOUSD"` (Brent Crude Oil), time is given in minutes and value is the closing price for Brent Crude Oil during that minute.
// MAGIC 
// MAGIC This data is historical data from 2010 to 2019 taken from https://www.histdata.com/ using methods from [FX-1-Minute-Data](https://github.com/philipperemy/FX-1-Minute-Data) by Philippe Remy. In this notebook, everything is done on static dataframes. See **02streamable-trend-calculus** for examples on streaming dataframes.
// MAGIC 
// MAGIC There are gaps in the data, notably during the weekends when no trading takes place, but this does not affect the algorithm as it is does not place any assumptions on the data other than that time is monotonically increasing.
// MAGIC 
// MAGIC The window size is set to 2, which is minimal, beacuse we want to retain as much information as possible.

// COMMAND ----------

val windowSize = 2
val oilDS = spark.read.fx1m("s3a://osint-gdelt-reado/canwrite/datasets/com/histdata/*.csv").toDF.withColumn("ticker", lit("BCOUSD")).select($"ticker", $"time" as "x", $"close" as "y").as[TickerPoint].orderBy("x")

// COMMAND ----------

// MAGIC %md
// MAGIC If we want to look at long term trends, we can use the output time series as input for another iteration. The output contains the points of the input where the trend changes (reversals). This can be repeated several times, resulting in longer term trends.
// MAGIC 
// MAGIC Here, we look at (up to) 15 iterations of the algorithm. It is no problem if the output of some iteration is too small to find a reversal in the next iteration, since the output will just be an empty dataframe in that case.

// COMMAND ----------

val numReversals = 15
val dfWithReversals = new TrendCalculus2(oilDS, windowSize, spark).nReversalsJoinedWithMaxRev(numReversals)

// COMMAND ----------

display(dfWithReversals)

// COMMAND ----------

// MAGIC %md
// MAGIC The number of reversals decrease rapidly as more iterations are done.

// COMMAND ----------

dfWithReversals.cache.count

// COMMAND ----------

(1 to numReversals).map( i => println(dfWithReversals.filter(s"reversal$i is not null").count))

// COMMAND ----------

// MAGIC %md
// MAGIC Writing dataframe to parquet in order to read from python.

// COMMAND ----------

dfWithReversals.write.mode(SaveMode.Overwrite).parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/trend-calculus-blog/public/joinedDSWithMaxRev")
dfWithReversals.unpersist

// COMMAND ----------

// MAGIC %md
// MAGIC ## Visualization
// MAGIC 
// MAGIC Plotly in python is used to make interactive visualizations.

// COMMAND ----------

// MAGIC %python
// MAGIC from plotly.offline import plot
// MAGIC from plotly.graph_objs import *
// MAGIC from datetime import *
// MAGIC joinedDS = spark.read.parquet("s3a://osint-gdelt-reado/canwrite/summerinterns2020/trend-calculus-blog/public/joinedDSWithMaxRev").orderBy("x")

// COMMAND ----------

// MAGIC %md
// MAGIC Seeing how much the timeseries has to be thinned out in order to display locally.
// MAGIC 
// MAGIC No information about higher order trend reversals is lost since every higher order reversal is also a lower order reversal.

// COMMAND ----------

// MAGIC %python
// MAGIC joinedDS.filter("maxRev > 2").count()

// COMMAND ----------

// MAGIC %python
// MAGIC fullTS = joinedDS.filter("maxRev > 2").select("x","y","maxRev").collect()

// COMMAND ----------

// MAGIC %md
// MAGIC Picking an interval to focus on.
// MAGIC 
// MAGIC Start and end dates as (year, month, day, hour, minute, second).
// MAGIC Only year, month and day are required.
// MAGIC The interval from 1800 to 2200 ensures all data is selected.

// COMMAND ----------

// MAGIC %python
// MAGIC startDate = datetime(1800,1,1)
// MAGIC endDate= datetime(2200,12,31)
// MAGIC TS = [row for row in fullTS if startDate <= row['x'] and row['x'] <= endDate]

// COMMAND ----------

// MAGIC %md
// MAGIC Setting up the visualization.

// COMMAND ----------

// MAGIC %python
// MAGIC numReversals = 15
// MAGIC startReversal = 7
// MAGIC 
// MAGIC allData = {'x': [row['x'] for row in TS], 'y': [row['y'] for row in TS], 'maxRev': [row['maxRev'] for row in TS]}
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

// MAGIC %md
// MAGIC ### Plotting result as plotly graph
// MAGIC 
// MAGIC The graph is interactive, one can drag to zoom in on an area (double-click to get back) and click on the legend to hide/show different series.

// COMMAND ----------

// MAGIC %python
// MAGIC p = plot(
// MAGIC   [Scattergl(x=allData['x'], y=allData['y'], mode='lines', name='Oil Price')] + markerPlots
// MAGIC   ,
// MAGIC   output_type='div'
// MAGIC )
// MAGIC 
// MAGIC displayHTML(p)