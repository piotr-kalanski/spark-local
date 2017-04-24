package com.datawizards.sparklocal

import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

object DataSetAPI {
  def apply[T: ClassTag](iterable: Iterable[T]) = new DataSetAPIScalaImpl(iterable)
  def apply[T: ClassTag](ds: Dataset[T]) = new DataSetAPISparkImpl(ds)
}

trait DataSetAPI[T] {
  def map[That: ClassTag: Manifest](map: T => That): DataSetAPI[That]
  def filter(p: T => Boolean): DataSetAPI[T]
  def count(): Long
  def foreach(f: (T) => Unit): Unit
  def foreachPartition(f: (Iterator[T]) => Unit): Unit
  def collect(): Array[T]
  def head(): T
  def head(n: Int): Array[T]
  def reduce(func: (T,T) => T): T
  def cache(): DataSetAPI[T]

  def take(n: Int): Array[T] = head(n)

  override def toString: String = collect().toSeq.toString

  override def equals(obj: scala.Any): Boolean = obj match {
    case d:DataSetAPI[T] => this.collect().sameElements(d.collect())
    case _ => false
  }
}
