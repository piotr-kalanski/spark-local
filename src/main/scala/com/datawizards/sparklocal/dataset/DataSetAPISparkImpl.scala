package com.datawizards.sparklocal.dataset

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class DataSetAPISparkImpl[T: ClassTag](data: Dataset[T]) extends DataSetAPI[T] {

  private def create[U: ClassTag](ds: Dataset[U]) = new DataSetAPISparkImpl(ds)

  override def map[That: ClassTag: Manifest](map: T => That): DataSetAPI[That] =
    create(data.map(map)(ExpressionEncoder[That]()))

  override def collect(): Array[T] = data.collect()

  override def filter(p: T => Boolean): DataSetAPI[T] =
    create(data.filter(p))

  override def count(): Long = data.count()

  override def foreach(f: (T) => Unit): Unit = data.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = data.foreachPartition(f)

  override def head(): T = data.head()

  override def head(n: Int): Array[T] = data.head(n)

  override def reduce(func: (T, T) => T): T = data.reduce(func)

  override def cache(): DataSetAPI[T] = create(data.cache())

  override def checkpoint(eager: Boolean): DataSetAPI[T] = create(data.checkpoint(eager))

  override def persist(newLevel: StorageLevel): DataSetAPI[T] = create(data.persist(newLevel))

  override def persist(): DataSetAPI[T] = create(data.persist())

  override def flatMap[U: ClassTag: Manifest](func: (T) => TraversableOnce[U]): DataSetAPI[U] = create(data.flatMap(func)(ExpressionEncoder[U]()))

  override def distinct(): DataSetAPI[T] = create(data.distinct)
}
