package com.datawizards.sparklocal.rdd

import scala.reflect.ClassTag

class RDDAPIScalaImpl[T: ClassTag](val iterable: Iterable[T]) extends RDDAPI[T] {
  private val data: Seq[T] = iterable.toSeq

  override def collect(): Array[T] = data.toArray

  override def map[That: ClassTag](map: (T) => That): RDDAPI[That] = create(data.map(map))

  private def create[U: ClassTag](data: Iterable[U]) = new RDDAPIScalaImpl(data)
}
