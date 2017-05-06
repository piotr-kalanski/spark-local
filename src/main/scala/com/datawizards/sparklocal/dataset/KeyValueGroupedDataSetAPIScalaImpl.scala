package com.datawizards.sparklocal.dataset

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class KeyValueGroupedDataSetAPIScalaImpl[K: ClassTag: TypeTag, T: ClassTag: TypeTag](data: Map[K,Seq[T]]) extends KeyValueGroupedDataSetAPI[K, T] {

  private def create[U: ClassTag: TypeTag](data: Map[K,Seq[U]]) = new KeyValueGroupedDataSetAPIScalaImpl(data)

  override def count(): DataSetAPI[(K, Long)] =
    DataSetAPI(data.mapValues(_.size.toLong))

  override def mapValues[W: ClassTag: TypeTag](func: (T) => W): KeyValueGroupedDataSetAPI[K, W] =
    create(data.mapValues(_.map(func)))

  override def mapGroups[U: ClassTag: TypeTag](f: (K, Iterator[T]) => U): DataSetAPI[U] =
    DataSetAPI(data.map{case (k, values) => f(k,values.iterator)})

  override def reduceGroups(f: (T, T) => T): DataSetAPI[(K, T)] =
    DataSetAPI(data.mapValues(_.reduce(f)))

  override def flatMapGroups[U: ClassTag: TypeTag](f: (K, Iterator[T]) => TraversableOnce[U]): DataSetAPI[U] =
    DataSetAPI(
      data
        .flatMap{case (k,vals) => f(k, vals.iterator)}
    )

}
