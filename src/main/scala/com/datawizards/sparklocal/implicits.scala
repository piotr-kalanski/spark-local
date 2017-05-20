package com.datawizards.sparklocal

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}

/**
  * Implicit methods available in Scala for converting
  * common Scala objects into Datasets
  */
object implicits extends SQLImplicits {
  override protected def _sqlContext: SQLContext =
    SparkSession.builder().getOrCreate().sqlContext
}
