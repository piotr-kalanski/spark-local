package com.datawizards.sparklocal.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object RDDAPI {
  def apply[T: ClassTag](iterable: Iterable[T]) = new RDDAPIScalaImpl(iterable)
  def apply[T: ClassTag](rdd: RDD[T]) = new RDDAPISparkImpl(rdd)
}

trait RDDAPI[T] {
  def collect(): Array[T]
  def map[That: ClassTag](map: T => That): RDDAPI[That]
  def filter(p: T => Boolean): RDDAPI[T]
  override def toString: String = collect().toSeq.toString

  override def equals(obj: scala.Any): Boolean = obj match {
    case d:RDDAPI[T] => this.collect().sameElements(d.collect())
    case _ => false
  }
}
