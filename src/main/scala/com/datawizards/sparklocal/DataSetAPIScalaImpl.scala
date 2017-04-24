package com.datawizards.sparklocal

import scala.reflect.ClassTag

class DataSetAPIScalaImpl[T: ClassTag](iterable: Iterable[T]) extends DataSetAPI[T] {
  private val data: Seq[T] = iterable.toSeq

  override def map[That: ClassTag: Manifest](map: T => That): DataSetAPI[That] =
    new DataSetAPIScalaImpl(data.map(map))

  override def collect(): Array[T] = data.toArray

  override def filter(p: T => Boolean): DataSetAPI[T] =
    new DataSetAPIScalaImpl(data.filter(p))

  override def count(): Long = data.size

  override def foreach(f: (T) => Unit): Unit = data.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = f(data.iterator)

  override def head(): T = data.head

  override def head(n: Int): Array[T] = data.take(n).toArray

  override def reduce(func: (T, T) => T): T = data.reduce(func)

  override def cache(): DataSetAPI[T] = this
}
