package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.dataset.agg.AggregationFunction
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

  def agg[U1: ClassTag](agg1: AggregationFunction[V, U1])
                                (implicit enc1: Encoder[U1]): DataSetAPI[(K,U1)]

  def agg[U1: ClassTag, U2: ClassTag](
                                                     agg1: AggregationFunction[V,U1],
                                                     agg2: AggregationFunction[V,U2]
                                                   )
                                                   (implicit
                                                    enc1: Encoder[U1],
                                                    enc2: Encoder[U2],
                                                    enc: Encoder[(U1,U2)]
                                                   ): DataSetAPI[(K,U1,U2)]

  def agg[U1: ClassTag, U2: ClassTag, U3: ClassTag](
                                                                   agg1: AggregationFunction[V,U1],
                                                                   agg2: AggregationFunction[V,U2],
                                                                   agg3: AggregationFunction[V,U3]
                                                                 )
                                                                 (implicit
                                                                  enc1: Encoder[U1],
                                                                  enc2: Encoder[U2],
                                                                  enc3: Encoder[U3],
                                                                  enc: Encoder[(U1,U2,U3)]
                                                                 ): DataSetAPI[(K,U1,U2,U3)]

  def agg[U1: ClassTag, U2: ClassTag, U3: ClassTag, U4: ClassTag](
                                                                   agg1: AggregationFunction[V,U1],
                                                                   agg2: AggregationFunction[V,U2],
                                                                   agg3: AggregationFunction[V,U3],
                                                                   agg4: AggregationFunction[V,U4]
                                                                 )
                                                                 (implicit
                                                                  enc1: Encoder[U1],
                                                                  enc2: Encoder[U2],
                                                                  enc3: Encoder[U3],
                                                                  enc4: Encoder[U4],
                                                                  enc: Encoder[(U1,U2,U3,U4)]
                                                                 ): DataSetAPI[(K,U1,U2,U3,U4)]
}
