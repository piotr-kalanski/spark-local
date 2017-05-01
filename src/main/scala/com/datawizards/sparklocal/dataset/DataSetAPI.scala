package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Dataset, SparkSession}
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

  override def toString: String = collect().toSeq.toString

  override def equals(obj: scala.Any): Boolean = obj match {
    case d:DataSetAPI[T] => this.collect().sameElements(d.collect())
    case _ => false
  }
}
