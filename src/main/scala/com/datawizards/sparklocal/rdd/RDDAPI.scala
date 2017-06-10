package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.impl.scala.`lazy`.rdd.{PairRDDFunctionsAPIScalaLazyImpl, RDDAPIScalaLazyImpl}
import com.datawizards.sparklocal.impl.scala.eager.rdd.{PairRDDFunctionsAPIScalaEagerImpl, RDDAPIScalaEagerImpl}
import com.datawizards.sparklocal.impl.scala.parallel.rdd.{PairRDDFunctionsAPIScalaParallelImpl, RDDAPIScalaParallelImpl}
import com.datawizards.sparklocal.impl.spark.rdd.{PairRDDFunctionsAPISparkImpl, RDDAPISparkImpl}
import org.apache.spark.{Partition, Partitioner}
import org.apache.spark.rdd.{PartitionCoalescer, RDD}
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.parallel.ParSeq
import scala.collection.{GenIterable, GenMap, Map, SeqView}
import scala.reflect.ClassTag

object RDDAPI {
  def apply[T: ClassTag](iterable: Iterable[T]): RDDAPI[T] = apply(iterable.toSeq)
  def apply[T: ClassTag](seq: Seq[T]): RDDAPI[T] = new RDDAPIScalaEagerImpl(seq)
  def apply[T: ClassTag](seq: SeqView[T, Seq[T]]): RDDAPI[T] = new RDDAPIScalaLazyImpl(seq)
  def apply[T: ClassTag](seq: ParSeq[T]): RDDAPI[T] = new RDDAPIScalaParallelImpl(seq)
  def apply[T: ClassTag](rdd: RDD[T]): RDDAPI[T] = new RDDAPISparkImpl(rdd)

  implicit def rddToPairRDDFunctions[K, V](rdd: RDDAPI[(K, V)])
    (implicit kct: ClassTag[K], vct: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctionsAPI[K, V] = {
     rdd match {
      case rddScala:RDDAPIScalaEagerImpl[(K,V)] => new PairRDDFunctionsAPIScalaEagerImpl(rddScala)(kct,vct,ord)
      case rddScala:RDDAPIScalaLazyImpl[(K,V)] => new PairRDDFunctionsAPIScalaLazyImpl(rddScala)(kct,vct,ord)
      case rddScala:RDDAPIScalaParallelImpl[(K,V)] => new PairRDDFunctionsAPIScalaParallelImpl(rddScala)(kct,vct,ord)
      case rddSpark:RDDAPISparkImpl[(K,V)] => new PairRDDFunctionsAPISparkImpl(rddSpark)(kct,vct,ord)
    }
  }

}

trait RDDAPI[T] {
  protected lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  protected def parallelize[That: ClassTag](d: Seq[That]): RDD[That] = spark.sparkContext.parallelize(d)
  protected def parallelize[That: ClassTag](d: GenIterable[That]): RDD[That] = parallelize(d.toSeq.seq)
  private[sparklocal] def toRDD: RDD[T]

  def collect(): Array[T]
  def map[That: ClassTag](map: T => That): RDDAPI[That]
  def flatMap[U: ClassTag](func: (T) => TraversableOnce[U]): RDDAPI[U]
  def filter(p: T => Boolean): RDDAPI[T]
  def reduce(func: (T,T) => T): T
  def fold(zeroValue: T)(op: (T, T) => T): T
  def head(): T
  def head(n: Int): Array[T]
  def take(n: Int): Array[T] = head(n)
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
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
  def ++(other: RDDAPI[T]): RDDAPI[T] = this.union(other)
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
  def subtract(other: RDDAPI[T]): RDDAPI[T]
  def subtract(other: RDDAPI[T], numPartitions: Int): RDDAPI[T]
  def subtract(other: RDDAPI[T], partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDDAPI[T]
  def countByValue()(implicit kct: ClassTag[T], vct: ClassTag[Int], ord: Ordering[T] = null): GenMap[T, Long] =
    RDDAPI.rddToPairRDDFunctions[T,Int](map(value => (value, 0))).countByKey()
  def keyBy[K](f: T => K)(implicit ct: ClassTag[K], kvct: ClassTag[(K,T)]): RDDAPI[(K, T)] =
    map(x => (f(x), x))
  def cartesian[U: ClassTag](other: RDDAPI[U]): RDDAPI[(T, U)]
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDDAPI[(K, GenIterable[T])]
  def groupBy[K](f: T => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDDAPI[(K, GenIterable[T])]
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null): RDDAPI[(K, GenIterable[T])]
  def coalesce(numPartitions: Int, shuffle: Boolean = false, partitionCoalescer: Option[PartitionCoalescer] = Option.empty)(implicit ord: Ordering[T] = null): RDDAPI[T]
  def sample(withReplacement: Boolean, fraction: Double, seed: Long = 0L): RDDAPI[T]
  def takeSample(withReplacement: Boolean, num: Int, seed: Long = 0L): Array[T]
  def randomSplit(weights: Array[Double], seed: Long = 0L): Array[RDDAPI[T]]
  def toDataSet(implicit enc: Encoder[T]): DataSetAPI[T]

  override def toString: String = "RDD(" + collect().mkString(",") + ")"

  override def equals(obj: scala.Any): Boolean = obj match {
    case d:RDDAPI[T] => this.collect().sameElements(d.collect())
    case _ => false
  }
}
