package com.datawizards.sparklocal.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RDDAPISparkImpl[T: ClassTag](val data: RDD[T]) extends RDDAPI[T] {

  private def create[U: ClassTag](rdd: RDD[U]) = new RDDAPISparkImpl(rdd)

  override def collect(): Array[T] = data.collect()

  override def map[That: ClassTag](map: (T) => That): RDDAPI[That] = create(data.map(map))

  override def filter(p: (T) => Boolean): RDDAPI[T] = create(data.filter(p))

  override def flatMap[U: ClassTag : Manifest](func: (T) => TraversableOnce[U]): RDDAPI[U] =
    create(data.flatMap(func))

  override def reduce(func: (T, T) => T): T = data.reduce(func)

  override def fold(zeroValue: T)(op: (T, T) => T): T = data.fold(zeroValue)(op)

  override def head(): T = data.first()

  override def head(n: Int): Array[T] = data.take(n)

  override def isEmpty: Boolean = data.isEmpty

  override def zip[U: ClassTag](other: RDDAPI[U]): RDDAPI[(T, U)] = other match {
    case rddScala:RDDAPIScalaImpl[U] => RDDAPI(data zip spark.sparkContext.parallelize(rddScala.data))
    case rddSpark:RDDAPISparkImpl[U] => create(data zip rddSpark.data)
  }

}
