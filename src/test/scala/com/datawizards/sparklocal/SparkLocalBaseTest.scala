package com.datawizards.sparklocal

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.FunSuite

trait SparkLocalBaseTest extends FunSuite {
  lazy val spark: SparkSession = {
    val r = SparkSession.builder().master("local").getOrCreate()
    r.sparkContext.setCheckpointDir("checkpoints/")
    r
  }
  lazy val sc: SparkContext = spark.sparkContext
  lazy val sqlContext: SQLContext = spark.sqlContext

  def assertDatasetOperationResult[T](ds: DataSetAPI[T])(expected: Array[T]): Unit = {
    assertResult(expected){
      ds.collect()
    }
  }

  def assertDatasetOperation[T:Manifest, Result](data: Seq[T])(op: DataSetAPI[T] => Result): Unit = {
    assertDatasetOperationWithEqual(data,op){case(r1,r2) => r1 == r2}
  }

  def assertDatasetOperationWithEqual[T:Manifest, Result](data: Seq[T], op: DataSetAPI[T] => Result)(eq: ((Result,Result) => Boolean)): Unit = {
    val ds = spark.createDataset(data)(ExpressionEncoder[T]())

    assert(eq(op(DataSetAPI(data)),op(DataSetAPI(ds))))
  }

}
