package com.datawizards.sparklocal.rdd

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RDDAPISparkImpl[T: ClassTag](val data: RDD[T]) extends RDDAPI[T] {

  private def create[U: ClassTag](rdd: RDD[U]) = new RDDAPISparkImpl(rdd)

  override def collect(): Array[T] = data.collect()

  override def map[That: ClassTag](map: (T) => That): RDDAPI[That] = create(data.map(map))

  override def filter(p: (T) => Boolean): RDDAPI[T] = create(data.filter(p))

}
