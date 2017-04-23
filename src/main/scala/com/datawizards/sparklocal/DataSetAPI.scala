package com.datawizards.sparklocal

import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

object DataSetAPI {
  def apply[T: ClassTag](iterable: Iterable[T]) = new DataSetAPIScalaImpl(iterable)
  def apply[T: ClassTag](ds: Dataset[T]) = new DataSetAPISparkImpl(ds)
}

trait DataSetAPI[T] {
  def map[That: ClassTag: Manifest](map: T => That): DataSetAPI[That]
  def filter(p: T => Boolean): DataSetAPI[T]
  def collect(): Array[T]

  override def equals(obj: scala.Any): Boolean = obj match {
    case d:DataSetAPI[T] => this.collect().sameElements(d.collect())
    case _ => false
  }
}
