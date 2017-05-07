package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.dataset.expr.Expressions._
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object DataSetAPI {
  def apply[T: ClassTag: TypeTag](iterable: Iterable[T]) = new DataSetAPIScalaImpl(iterable)
  def apply[T: ClassTag: TypeTag](ds: Dataset[T]) = new DataSetAPISparkImpl(ds)
}

trait DataSetAPI[T] {
  protected lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  protected def createDataset[That: TypeTag](d: Seq[That]): Dataset[That] = {
    implicit val encoder = ExpressionEncoder[That]()
    spark.createDataset(d)
  }
  private[dataset] def toDataset: Dataset[T]

  def map[That: ClassTag: TypeTag](map: T => That): DataSetAPI[That]
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
  def flatMap[U: ClassTag: TypeTag](func: (T) => TraversableOnce[U]): DataSetAPI[U]
  def distinct(): DataSetAPI[T]
  def rdd(): RDDAPI[T]
  def union(other: DataSetAPI[T]): DataSetAPI[T]
  def intersect(other: DataSetAPI[T]): DataSetAPI[T]
  def groupByKey[K: ClassTag: TypeTag](func: (T) => K): KeyValueGroupedDataSetAPI[K, T]
  def limit(n: Int): DataSetAPI[T]
  def repartition(numPartitions: Int): DataSetAPI[T]
  def repartition(partitionExprs: Column*): DataSetAPI[T]
  def repartition(numPartitions: Int, partitionExprs: Column*): DataSetAPI[T]
  def coalesce(numPartitions: Int): DataSetAPI[T]
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataSetAPI[T]
  def randomSplit(weights: Array[Double], seed: Long = 0L): Array[DataSetAPI[T]]
  def join[K: ClassTag: TypeTag,W: ClassTag: TypeTag](other: DataSetAPI[W])(left: T=>K, right: W=>K)(implicit ct: ClassTag[T], tg: TypeTag[T]): DataSetAPI[(T, W)] = {
    val leftRDD = this.map(x => (left(x), x)).rdd()
    val rightRDD = other.map(x => (right(x), x)).rdd()
    RDDAPI.rddToPairRDDFunctions(leftRDD)
      .join(rightRDD)
      .map(p => p._2)
      .toDataSet
  }
  def leftOuterJoin[K: ClassTag: TypeTag,W: ClassTag: TypeTag](other: DataSetAPI[W])(left: T=>K, right: W=>K)(implicit ct: ClassTag[T], tg: TypeTag[T]): DataSetAPI[(T, Option[W])] = {
    val leftRDD = this.map(x => (left(x), x)).rdd()
    val rightRDD = other.map(x => (right(x), x)).rdd()
    RDDAPI.rddToPairRDDFunctions(leftRDD)
      .leftOuterJoin(rightRDD)
      .map(p => p._2)
      .toDataSet
  }
  def rightOuterJoin[K: ClassTag: TypeTag,W: ClassTag: TypeTag](other: DataSetAPI[W])(left: T=>K, right: W=>K)(implicit ct: ClassTag[T], tg: TypeTag[T]): DataSetAPI[(Option[T], W)] = {
    val leftRDD = this.map(x => (left(x), x)).rdd()
    val rightRDD = other.map(x => (right(x), x)).rdd()
    RDDAPI.rddToPairRDDFunctions(leftRDD)
      .rightOuterJoin(rightRDD)
      .map(p => p._2)
      .toDataSet
  }
  def fullOuterJoin[K: ClassTag: TypeTag,W: ClassTag: TypeTag](other: DataSetAPI[W])(left: T=>K, right: W=>K)(implicit ct: ClassTag[T], tg: TypeTag[T]): DataSetAPI[(Option[T], Option[W])] = {
    val leftRDD = this.map(x => (left(x), x)).rdd()
    val rightRDD = other.map(x => (right(x), x)).rdd()
    RDDAPI.rddToPairRDDFunctions(leftRDD)
      .fullOuterJoin(rightRDD)
      .map(p => p._2)
      .toDataSet
  }
  //TODO - uncomment
  //def joinWith[U](DataSetAPI: Dataset[U], condition: BooleanExpression, joinType: String): DataSetAPI[(T, U)]

  override def toString: String = "DataSet(" + collect().mkString(",") + ")"

  override def equals(obj: scala.Any): Boolean = obj match {
    case d:DataSetAPI[T] => this.collect().sameElements(d.collect())
    case _ => false
  }
}
