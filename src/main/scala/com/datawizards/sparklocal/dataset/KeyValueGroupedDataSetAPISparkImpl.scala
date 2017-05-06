package com.datawizards.sparklocal.dataset

import org.apache.spark.sql.KeyValueGroupedDataset
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class KeyValueGroupedDataSetAPISparkImpl[K: ClassTag: TypeTag, T: ClassTag: TypeTag](private[dataset] val data: KeyValueGroupedDataset[K, T]) extends KeyValueGroupedDataSetAPI[K, T] {
  private def create[U: ClassTag: TypeTag](data: KeyValueGroupedDataset[K,U]) = new KeyValueGroupedDataSetAPISparkImpl(data)

  override private[dataset] def toKeyValueGroupedDataSet = data

  override def count(): DataSetAPI[(K, Long)] =
    DataSetAPI(data.count())

  override def mapValues[W: ClassTag: TypeTag](func: (T) => W): KeyValueGroupedDataSetAPI[K, W] =
    create(data.mapValues(func)(ExpressionEncoder[W]()))

  override def mapGroups[U: ClassTag: TypeTag](f: (K, Iterator[T]) => U): DataSetAPI[U] =
    DataSetAPI(data.mapGroups(f)(ExpressionEncoder[U]()))

  override def reduceGroups(f: (T, T) => T): DataSetAPI[(K, T)] =
    DataSetAPI(data.reduceGroups(f))

  override def flatMapGroups[U: ClassTag: TypeTag](f: (K, Iterator[T]) => TraversableOnce[U]): DataSetAPI[U] =
    DataSetAPI(data.flatMapGroups(f)(ExpressionEncoder[U]()))

  override def keys: DataSetAPI[K] =
    DataSetAPI(data.keys)

  override def cogroup[U: ClassTag : TypeTag, R: ClassTag : TypeTag](other: KeyValueGroupedDataSetAPI[K, U])(f: (K, Iterator[T], Iterator[U]) => TraversableOnce[R]): DataSetAPI[R] = {
    implicit val encoder = ExpressionEncoder[R]()
    DataSetAPI(data.cogroup(other.toKeyValueGroupedDataSet)(f))
  }


}
