package com.datawizards.sparklocal.dataset

import java.util

import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class DataSetAPIScalaImpl[T: ClassTag: TypeTag](iterable: Iterable[T]) extends DataSetAPI[T] {
  private val data: Seq[T] = iterable.toSeq

  private def create[U: ClassTag: TypeTag](it: Iterable[U]) = new DataSetAPIScalaImpl(it)

  override private[dataset] def toDataset = createDataset(data)

  override def map[That: ClassTag: TypeTag](map: T => That): DataSetAPI[That] = create(data.map(map))

  override def collect(): Array[T] = data.toArray

  override def collectAsList(): java.util.List[T] = data

  override def filter(p: T => Boolean): DataSetAPI[T] = create(data.filter(p))

  override def count(): Long = data.size

  override def foreach(f: (T) => Unit): Unit = data.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = f(data.iterator)

  override def head(): T = data.head

  override def head(n: Int): Array[T] = data.take(n).toArray

  override def reduce(func: (T, T) => T): T = data.reduce(func)

  override def cache(): DataSetAPI[T] = this

  override def checkpoint(eager: Boolean): DataSetAPI[T] = this

  override def persist(newLevel: StorageLevel): DataSetAPI[T] = this

  override def persist(): DataSetAPI[T] = this

  override def unpersist(): DataSetAPI[T] = this

  override def unpersist(blocking: Boolean): DataSetAPI[T] = this

  override def flatMap[U: ClassTag: TypeTag](func: (T) => TraversableOnce[U]): DataSetAPI[U] = create(data.flatMap(func))

  override def distinct(): DataSetAPI[T] = create(data.distinct)

  override def rdd(): RDDAPI[T] = RDDAPI(data)

  override def union(other: DataSetAPI[T]): DataSetAPI[T] = other match {
    case dsSpark:DataSetAPISparkImpl[T] => DataSetAPI(this.toDataset.union(dsSpark.data))
    case dsScala:DataSetAPIScalaImpl[T] => create(data.union(dsScala.data))
  }

  override def intersect(other: DataSetAPI[T]): DataSetAPI[T] = other match {
    case dsSpark:DataSetAPISparkImpl[T] => DataSetAPI(this.toDataset.intersect(dsSpark.data))
    case dsScala:DataSetAPIScalaImpl[T] => create(data.intersect(dsScala.data))
  }

  override def takeAsList(n: Int): util.List[T] = data.take(n)

  override def groupByKey[K: TypeTag](func: (T) => K): KeyValueGroupedDataSetAPI[K, T] =
    new KeyValueGroupedDataSetAPIScalaImpl(data.groupBy(func))
}
