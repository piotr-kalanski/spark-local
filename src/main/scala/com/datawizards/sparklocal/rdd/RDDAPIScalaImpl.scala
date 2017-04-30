package com.datawizards.sparklocal.rdd

import scala.reflect.ClassTag

class RDDAPIScalaImpl[T: ClassTag](val iterable: Iterable[T]) extends RDDAPI[T] {
  private val data: Seq[T] = iterable.toSeq

  private def create[U: ClassTag](data: Iterable[U]) = new RDDAPIScalaImpl(data)

  override def collect(): Array[T] = data.toArray

  override def map[That: ClassTag](map: (T) => That): RDDAPI[That] = create(data.map(map))

  override def filter(p: (T) => Boolean): RDDAPI[T] = create(data.filter(p))

  override def flatMap[U: ClassTag : Manifest](func: (T) => TraversableOnce[U]): RDDAPI[U] =
    create(data.flatMap(func))

  override def reduce(func: (T, T) => T): T = data.reduce(func)

  override def fold(zeroValue: T)(op: (T, T) => T): T = data.fold(zeroValue)(op)

  override def head(): T = data.head

  override def head(n: Int): Array[T] = data.take(n).toArray

  override def isEmpty: Boolean = data.isEmpty
}
