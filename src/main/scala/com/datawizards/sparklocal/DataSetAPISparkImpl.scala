package com.datawizards.sparklocal

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag

class DataSetAPISparkImpl[T: ClassTag](ds: Dataset[T]) extends DataSetAPI[T] {
  private lazy val spark: SparkSession = SparkSession.builder().getOrCreate()

  override def map[That: ClassTag: Manifest](map: T => That): DataSetAPI[That] =
    new DataSetAPISparkImpl(ds.map(map)(ExpressionEncoder[That]()))

  override def collect(): Array[T] = ds.collect()

  override def toString: String = collect().toString

  override def filter(p: T => Boolean): DataSetAPI[T] =
    new DataSetAPISparkImpl(ds.filter(p))

}
