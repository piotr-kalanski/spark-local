package com.datawizards.sparklocal

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

class DataSetAPISparkImpl[T: ClassTag](ds: Dataset[T]) extends DataSetAPI[T] {

  override def map[That: ClassTag: Manifest](map: T => That): DataSetAPI[That] =
    new DataSetAPISparkImpl(ds.map(map)(ExpressionEncoder[That]()))

  override def collect(): Array[T] = ds.collect()

  override def filter(p: T => Boolean): DataSetAPI[T] =
    new DataSetAPISparkImpl(ds.filter(p))

  override def count(): Long = ds.count()

  override def foreach(f: (T) => Unit): Unit = ds.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = ds.foreachPartition(f)

  override def head(): T = ds.head()

  override def head(n: Int): Array[T] = ds.head(n)

  override def reduce(func: (T, T) => T): T = ds.reduce(func)

  override def cache(): DataSetAPI[T] = new DataSetAPISparkImpl(ds.cache())
}
