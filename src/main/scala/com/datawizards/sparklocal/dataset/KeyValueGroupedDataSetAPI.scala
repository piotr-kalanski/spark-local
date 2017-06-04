package com.datawizards.sparklocal.dataset

import org.apache.spark.sql.{Encoder, KeyValueGroupedDataset, SparkSession}

import scala.reflect.ClassTag

trait KeyValueGroupedDataSetAPI[K, V] {
  protected lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  private[sparklocal] def toKeyValueGroupedDataSet(implicit encK: Encoder[K], encT: Encoder[V], encKT: Encoder[(K, V)]): KeyValueGroupedDataset[K, V]

  def count(): DataSetAPI[(K, Long)]
  def mapGroups[U: ClassTag](f: (K, Iterator[V]) => U)
                            (implicit enc: Encoder[U]=null): DataSetAPI[U]
  def reduceGroups(f: (V, V) => V): DataSetAPI[(K, V)]
  def mapValues[W: ClassTag](func: V => W)
                            (implicit enc: Encoder[W]=null): KeyValueGroupedDataSetAPI[K, W]
  def flatMapGroups[U: ClassTag](f: (K, Iterator[V]) => TraversableOnce[U])
                                (implicit enc: Encoder[U]=null): DataSetAPI[U]
  def keys: DataSetAPI[K]
  def cogroup[U: ClassTag, R: ClassTag](other: KeyValueGroupedDataSetAPI[K, U])
                                       (f: (K, Iterator[V], Iterator[U]) => TraversableOnce[R])
                                       (implicit
                                          encK: Encoder[K]=null,
                                          encV: Encoder[V]=null,
                                          encU: Encoder[U]=null,
                                          encR: Encoder[R]=null,
                                          encKV: Encoder[(K,V)]=null,
                                          encKU: Encoder[(K,U)]=null
                                       ): DataSetAPI[R]
}
