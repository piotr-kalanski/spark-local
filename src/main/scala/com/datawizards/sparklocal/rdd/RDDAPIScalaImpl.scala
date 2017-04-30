package com.datawizards.sparklocal.rdd

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class RDDAPIScalaImpl[T: ClassTag](val iterable: Iterable[T]) extends RDDAPI[T] {
  private[rdd] val data: Seq[T] = iterable.toSeq

  private def create[U: ClassTag](data: Iterable[U]) = new RDDAPIScalaImpl(data)

  override def collect(): Array[T] = data.toArray

  override def map[That: ClassTag](map: (T) => That): RDDAPI[That] = create(data.map(map))

  override def filter(p: (T) => Boolean): RDDAPI[T] = create(data.filter(p))

  override def flatMap[U: ClassTag : Manifest](func: (T) => TraversableOnce[U]): RDDAPI[U] =
    create(data.flatMap(func))

  override def reduce(func: (T, T) => T): T = data.reduce(func)

  override def fold(zeroValue: T)(op: (T, T) => T): T = data.fold(zeroValue)(op)

  override def head(): T = data.head

  override def head(n: Int): Array[T] = data.take(n).toArray

  override def isEmpty: Boolean = data.isEmpty

  override def zip[U: ClassTag](other: RDDAPI[U]): RDDAPI[(T, U)] = other match {
    case rddScala:RDDAPIScalaImpl[U] => create(data zip rddScala.data)
    case rddSpark:RDDAPISparkImpl[U] => RDDAPI(spark.sparkContext.parallelize(data) zip rddSpark.data)
  }

  override def foreach(f: (T) => Unit): Unit = data.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = f(data.iterator)

  override def checkpoint(): RDDAPI[T] = this

  override def cache(): RDDAPI[T] = this

  override def persist(newLevel: StorageLevel): RDDAPI[T] = this

  override def persist(): RDDAPI[T] = this

  override def union(other: RDDAPI[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaImpl[T] => create(data union rddScala.data)
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(spark.sparkContext.parallelize(data) union rddSpark.data)
  }

  override def zipWithIndex(): RDDAPI[(T, Long)] = create(data zip (0L until data.size))

  override def min()(implicit ord: Ordering[T]): T = data.min

  override def max()(implicit ord: Ordering[T]): T = data.max

  override def partitions: Array[Partition] = Array.empty

  override def sortBy[K](f: (T) => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDDAPI[T] =
    create(data.sortBy(f)(if(ascending) ord else ord.reverse))

}
