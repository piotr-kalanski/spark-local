package com.datawizards.sparklocal

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}


// TODO - czy to tutaj zostawiac - jest to troche mylace, bo to tylko pod Spark implementacja, co prawda w kontekscie Scali jedynie spowalnia
/**
  * Implicit methods available in Scala for converting
  * common Scala objects into Datasets
  */
object implicits extends SQLImplicits {
  override protected def _sqlContext: SQLContext =
    SparkSession.builder().getOrCreate().sqlContext
}
