package com.datawizards.sparklocal.dataset

import org.apache.spark.sql.KeyValueGroupedDataset
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class KeyValueGroupedDataSetAPISparkImpl[K: TypeTag, T: TypeTag](data: KeyValueGroupedDataset[K, T]) extends KeyValueGroupedDataSetAPI[K, T] {
  private def create[U: TypeTag](data: KeyValueGroupedDataset[K,U]) = new KeyValueGroupedDataSetAPISparkImpl(data)

  override def count(): DataSetAPI[(K, Long)] = DataSetAPI(data.count())

  override def mapValues[W: ClassTag: TypeTag](func: (T) => W): KeyValueGroupedDataSetAPI[K, W] = create(data.mapValues(func)(ExpressionEncoder[W]()))

  override def mapGroups[U: ClassTag: TypeTag](f: (K, Iterator[T]) => U): DataSetAPI[U] = DataSetAPI(data.mapGroups(f)(ExpressionEncoder[U]()))

  override def reduceGroups(f: (T, T) => T): DataSetAPI[(K, T)] = DataSetAPI(data.reduceGroups(f))

}
