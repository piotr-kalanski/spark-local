package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.dataset.expressions.Expressions._
import com.datawizards.sparklocal.dataset.io.WriterExecutor
import com.datawizards.sparklocal.impl.scala.`lazy`.dataset.DataSetAPIScalaLazyImpl
import com.datawizards.sparklocal.impl.scala.eager.dataset.DataSetAPIScalaEagerImpl
import com.datawizards.sparklocal.impl.scala.parallel.dataset.DataSetAPIScalaParallelImpl
import com.datawizards.sparklocal.impl.scala.parallellazy.ParallelLazySeq
import com.datawizards.sparklocal.impl.scala.parallellazy.dataset.DataSetAPIScalaParallelLazyImpl
import com.datawizards.sparklocal.impl.spark.dataset.DataSetAPISparkImpl
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.SeqView
import scala.collection.parallel.ParSeq
import scala.reflect.ClassTag

object DataSetAPI {
  def apply[T: ClassTag](iterable: Iterable[T]): DataSetAPI[T] =
    apply(iterable.toSeq)
  def apply[T: ClassTag](seq: Seq[T]): DataSetAPI[T] =
    new DataSetAPIScalaEagerImpl(seq)
  def apply[T: ClassTag](data: SeqView[T, Seq[T]]): DataSetAPI[T] =
    new DataSetAPIScalaLazyImpl(data)
  def apply[T: ClassTag](seq: ParSeq[T]): DataSetAPI[T] =
    new DataSetAPIScalaParallelImpl(seq)
  def apply[T: ClassTag](data: ParallelLazySeq[T]): DataSetAPI[T] =
    new DataSetAPIScalaParallelLazyImpl(data)
  def apply[T: ClassTag](ds: Dataset[T]): DataSetAPI[T] = new DataSetAPISparkImpl(ds)
}

trait DataSetAPI[T] {
  protected lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  protected def createDataset[That](d: Seq[That])(implicit enc: Encoder[That]): Dataset[That] =
    spark.createDataset(d)

  private[sparklocal] def toDataset(implicit enc: Encoder[T]): Dataset[T]

  def map[That: ClassTag](map: T => That)(implicit enc: Encoder[That]): DataSetAPI[That]
  def filter(p: T => Boolean): DataSetAPI[T]
  def count(): Long
  def foreach(f: (T) => Unit): Unit
  def foreachPartition(f: (Iterator[T]) => Unit): Unit
  def collect(): Array[T]
  def collectAsList(): java.util.List[T]
  def head(): T
  def head(n: Int): Array[T]
  def take(n: Int): Array[T] = head(n)
  def takeAsList(n: Int): java.util.List[T]
  def reduce(func: (T,T) => T): T
  def checkpoint(eager: Boolean): DataSetAPI[T]
  def checkpoint(): DataSetAPI[T] = checkpoint(true)
  def cache(): DataSetAPI[T]
  def persist(newLevel: StorageLevel): DataSetAPI[T]
  def persist(): DataSetAPI[T]
  def unpersist(): DataSetAPI[T]
  def unpersist(blocking: Boolean): DataSetAPI[T]
  def flatMap[U: ClassTag](func: (T) => TraversableOnce[U])(implicit enc: Encoder[U]=null): DataSetAPI[U]
  def distinct(): DataSetAPI[T]
  def rdd(): RDDAPI[T]
  def union(other: DataSetAPI[T])(implicit enc: Encoder[T]): DataSetAPI[T]
  def intersect(other: DataSetAPI[T])(implicit enc: Encoder[T]): DataSetAPI[T]
  def groupByKey[K: ClassTag](func: (T) => K)(implicit enc: Encoder[K]=null): KeyValueGroupedDataSetAPI[K, T]
  def limit(n: Int): DataSetAPI[T]
  def repartition(numPartitions: Int): DataSetAPI[T]
  def repartition(partitionExprs: Column*): DataSetAPI[T]
  def repartition(numPartitions: Int, partitionExprs: Column*): DataSetAPI[T]
  def coalesce(numPartitions: Int): DataSetAPI[T]
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataSetAPI[T]
  def randomSplit(weights: Array[Double], seed: Long = 0L): Array[DataSetAPI[T]]
  def join[K: ClassTag, W: ClassTag](other: DataSetAPI[W])(left: T=>K, right: W=>K)
                                    (implicit
                                     ct: ClassTag[T],
                                     encT: Encoder[T],
                                     encK: Encoder[K],
                                     encW: Encoder[W],
                                     encKT: Encoder[(K,T)],
                                     encKW: Encoder[(K,W)],
                                     encTW: Encoder[(T,W)]): DataSetAPI[(T, W)] = {
    val leftRDD = this.map(x => (left(x), x)).rdd()
    val rightRDD = other.map(x => (right(x), x)).rdd()
    RDDAPI.rddToPairRDDFunctions(leftRDD)
      .join(rightRDD)
      .map(p => p._2)
      .toDataSet
  }
  def leftOuterJoin[K: ClassTag, W: ClassTag](other: DataSetAPI[W])(left: T=>K, right: W=>K)
                                             (implicit
                                              ct: ClassTag[T],
                                              encT: Encoder[T],
                                              encK: Encoder[K],
                                              encW: Encoder[W],
                                              encKT: Encoder[(K,T)],
                                              encKW: Encoder[(K,W)],
                                              encTW: Encoder[(T,Option[W])]): DataSetAPI[(T, Option[W])] = {
    val leftRDD = this.map(x => (left(x), x)).rdd()
    val rightRDD = other.map(x => (right(x), x)).rdd()
    RDDAPI.rddToPairRDDFunctions(leftRDD)
      .leftOuterJoin(rightRDD)
      .map(p => p._2)
      .toDataSet
  }
  def rightOuterJoin[K: ClassTag, W: ClassTag](other: DataSetAPI[W])(left: T=>K, right: W=>K)
                                              (implicit
                                               ct: ClassTag[T],
                                               encT: Encoder[T],
                                               encK: Encoder[K],
                                               encW: Encoder[W],
                                               encKT: Encoder[(K,T)],
                                               encKW: Encoder[(K,W)],
                                               encTW: Encoder[(Option[T],W)]): DataSetAPI[(Option[T], W)] = {
    val leftRDD = this.map(x => (left(x), x)).rdd()
    val rightRDD = other.map(x => (right(x), x)).rdd()
    RDDAPI.rddToPairRDDFunctions(leftRDD)
      .rightOuterJoin(rightRDD)
      .map(p => p._2)
      .toDataSet
  }
  def fullOuterJoin[K: ClassTag, W: ClassTag](other: DataSetAPI[W])(left: T=>K, right: W=>K)
                                             (implicit
                                              ct: ClassTag[T],
                                              encT: Encoder[T],
                                              encK: Encoder[K],
                                              encW: Encoder[W],
                                              encKT: Encoder[(K,T)],
                                              encKW: Encoder[(K,W)],
                                              encTW: Encoder[(Option[T],Option[W])]): DataSetAPI[(Option[T], Option[W])] = {
    val leftRDD = this.map(x => (left(x), x)).rdd()
    val rightRDD = other.map(x => (right(x), x)).rdd()
    RDDAPI.rddToPairRDDFunctions(leftRDD)
      .fullOuterJoin(rightRDD)
      .map(p => p._2)
      .toDataSet
  }
  def field(name: String): Field = new Field(name)
  def apply(name: String): Field = field(name)
  def join[U: ClassTag](other: DataSetAPI[U], condition: BooleanExpression)
                       (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)]
  def leftOuterJoin[U: ClassTag](other: DataSetAPI[U], condition: BooleanExpression)
                                (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)]
  def rightOuterJoin[U: ClassTag](other: DataSetAPI[U], condition: BooleanExpression)
                                 (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)]
  def fullOuterJoin[U: ClassTag](other: DataSetAPI[U], condition: BooleanExpression)
                                (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)]
  def write: WriterExecutor[T]
  def show: WriterExecutor[T] = write
  def except(other: DataSetAPI[T])(implicit enc: Encoder[T]): DataSetAPI[T] = diff(other)
  def diff(other: DataSetAPI[T])(implicit enc: Encoder[T]): DataSetAPI[T]

  override def toString: String = "DataSet(" + collect().mkString(",") + ")"

  override def equals(obj: scala.Any): Boolean = obj match {
    case d:DataSetAPI[T] => this.collect().sameElements(d.collect())
    case _ => false
  }
}
