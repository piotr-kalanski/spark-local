package com.datawizards.sparklocal.rdd

import org.apache.spark.{Partition, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object RDDAPI {
  def apply[T: ClassTag](iterable: Iterable[T]) = new RDDAPIScalaImpl(iterable)
  def apply[T: ClassTag](rdd: RDD[T]) = new RDDAPISparkImpl(rdd)

  implicit def rddToPairRDDFunctions[K, V](rdd: RDDAPI[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctionsAPI[K, V] = {
    rdd match {
      case rddScala:RDDAPIScalaImpl[(K,V)] => new PairRDDFunctionsAPIScalaImpl(rddScala)(kt,vt,ord)
      case rddSpark:RDDAPISparkImpl[(K,V)] => new PairRDDFunctionsAPISparkImpl(rddSpark)(kt,vt,ord)
      case _ => throw new Exception("Unknown type")
    }
  }

}

trait RDDAPI[T] {
  protected lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  protected def parallelize[That: ClassTag](d: Seq[That]): RDD[That] = spark.sparkContext.parallelize(d)
  private[rdd] def toRDD: RDD[T]

  def collect(): Array[T]
  def map[That: ClassTag](map: T => That): RDDAPI[That]
  def flatMap[U: ClassTag](func: (T) => TraversableOnce[U]): RDDAPI[U]
  def filter(p: T => Boolean): RDDAPI[T]
  def reduce(func: (T,T) => T): T
  def fold(zeroValue: T)(op: (T, T) => T): T
  def head(): T
  def head(n: Int): Array[T]
  def take(n: Int): Array[T] = head(n)
  def first(): T = head()
  def isEmpty: Boolean
  def zip[U: ClassTag](other: RDDAPI[U]): RDDAPI[(T, U)]
  def foreach(f: (T) => Unit): Unit
  def foreachPartition(f: (Iterator[T]) => Unit): Unit
  def checkpoint(): RDDAPI[T]
  def cache(): RDDAPI[T]
  def persist(newLevel: StorageLevel): RDDAPI[T]
  def persist(): RDDAPI[T]
  def unpersist(blocking: Boolean = true): RDDAPI[T]
  def union(other: RDDAPI[T]): RDDAPI[T]
  def zipWithIndex(): RDDAPI[(T, Long)]
  def min()(implicit ord: Ordering[T]): T
  def max()(implicit ord: Ordering[T]): T
  def partitions: Array[Partition]
  def sortBy[K](f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDDAPI[T]
  def intersection(other: RDDAPI[T]): RDDAPI[T]
  def intersection(other: RDDAPI[T], numPartitions: Int): RDDAPI[T]
  def intersection(other: RDDAPI[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDDAPI[T]
  def count(): Long
  def distinct(): RDDAPI[T]
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDDAPI[T]
  def top(num: Int)(implicit ord: Ordering[T]): Array[T]

  override def toString: String = collect().toSeq.toString

  override def equals(obj: scala.Any): Boolean = obj match {
    case d:RDDAPI[T] => this.collect().sameElements(d.collect())
    case _ => false
  }
}
