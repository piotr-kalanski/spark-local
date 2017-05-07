package com.datawizards.sparklocal.dataset

import org.apache.spark.sql.{KeyValueGroupedDataset, SparkSession}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait KeyValueGroupedDataSetAPI[K, V] {
  protected lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  private[dataset] def toKeyValueGroupedDataSet: KeyValueGroupedDataset[K, V]

  def count(): DataSetAPI[(K, Long)]
  def mapGroups[U: ClassTag: TypeTag](f: (K, Iterator[V]) => U): DataSetAPI[U]
  def reduceGroups(f: (V, V) => V): DataSetAPI[(K, V)]
  def mapValues[W: ClassTag: TypeTag](func: V => W): KeyValueGroupedDataSetAPI[K, W]
  def flatMapGroups[U: ClassTag: TypeTag](f: (K, Iterator[V]) => TraversableOnce[U]): DataSetAPI[U]
  def keys: DataSetAPI[K]
  def cogroup[U: ClassTag: TypeTag, R: ClassTag: TypeTag](other: KeyValueGroupedDataSetAPI[K, U])(f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R]): DataSetAPI[R]
}
