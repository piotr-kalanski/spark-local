package com.datawizards.sparklocal

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.FunSuite

import scala.math.Ordering

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

  def assertRDDOperationResult[T](ds: RDDAPI[T])(expected: Array[T]): Unit = {
    assertResult(expected){
      ds.collect()
    }
  }

  def assertRDDOperation[T:Manifest, Result](data: Seq[T])(op: RDDAPI[T] => Result): Unit = {
    assertRDDOperationWithEqual(data,op){case(r1,r2) => r1 == r2}
  }

  def assertRDDOperationWithEqual[T:Manifest, Result](data: Seq[T], op: RDDAPI[T] => Result)(eq: ((Result,Result) => Boolean)): Unit = {
    val rdd = sc.parallelize(data)

    assert(eq(op(RDDAPI(data)),op(RDDAPI(rdd))))
  }

  def assertRDDOperationWithSortedResult[T:Manifest](data: Seq[T])(op: RDDAPI[T] => RDDAPI[T])(implicit ord: Ordering[T]): Unit = {
    assertRDDOperationWithEqual[T,RDDAPI[T]](data, op) {
      case (d1,d2) => d1.collect().sorted(ord) sameElements d2.collect().sorted(ord)
    }
  }

}
