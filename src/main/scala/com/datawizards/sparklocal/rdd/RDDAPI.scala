package com.datawizards.sparklocal.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object RDDAPI {
  def apply[T: ClassTag](iterable: Iterable[T]) = new RDDAPIScalaImpl(iterable)
  def apply[T: ClassTag](rdd: RDD[T]) = new RDDAPISparkImpl(rdd)
}

trait RDDAPI[T] {
  protected lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  def collect(): Array[T]
  def map[That: ClassTag](map: T => That): RDDAPI[That]
  def flatMap[U: ClassTag: Manifest](func: (T) ⇒ TraversableOnce[U]): RDDAPI[U]
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
  def union(other: RDDAPI[T]): RDDAPI[T]
  def zipWithIndex(): RDDAPI[(T, Long)]
  def min()(implicit ord: Ordering[T]): T
  def max()(implicit ord: Ordering[T]): T

  override def toString: String = collect().toSeq.toString

  override def equals(obj: scala.Any): Boolean = obj match {
    case d:RDDAPI[T] => this.collect().sameElements(d.collect())
    case _ => false
  }
}
