package com.datawizards.sparklocal

import scala.reflect.ClassTag

class DataSetAPIScalaImpl[T: ClassTag](iterable: Iterable[T]) extends DataSetAPI[T] {
  private val data: Seq[T] = iterable.toSeq

  override def map[That: ClassTag: Manifest](map: T => That): DataSetAPI[That] =
    new DataSetAPIScalaImpl(data.map(map))

  override def collect(): Array[T] = data.toArray

  override def toString: String = data.toString

  override def filter(p: T => Boolean): DataSetAPI[T] =
    new DataSetAPIScalaImpl(data.filter(p))

}
