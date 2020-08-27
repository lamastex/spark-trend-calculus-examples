// Databricks notebook source
// MAGIC %md
// MAGIC # Trend Calculus of OIL Price
// MAGIC 
// MAGIC Johannes Graner, Albert Nilsson and Raazesh Sainudiin
// MAGIC 
// MAGIC 2020, Uppsala, Sweden
// MAGIC 
// MAGIC This work was inspired by Antoine Aamennd's texata-2017 repository forked here:
// MAGIC 
// MAGIC - https://github.com/lamastex/spark-texata-2020/
// MAGIC 
// MAGIC and Andrew Morgan's Trend Calculus Library extended and adapted for Spark structured streams here:
// MAGIC 
// MAGIC - https://github.com/lamastex/spark-trend-calculus
// MAGIC 
// MAGIC 
// MAGIC This project was supported by Combient Mix AB through summer internships at:
// MAGIC 
// MAGIC Combient Competence Centre for Data Engineering Sciences, 
// MAGIC Department of Mathematics, 
// MAGIC Uppsala University, Uppsala, Sweden

// COMMAND ----------

// MAGIC %md
// MAGIC # Notebooks 
// MAGIC 
// MAGIC - [Showcasing Trend Calculus](notebooks/db/01trend-calculus-showcase.md)
// MAGIC - [Streaming Trend Calculus](notebooks/db/02streamable-trend-calculus.md)
// MAGIC - [Markov Model](notebooks/db/03streamable-trend-calculus-estimators.md)