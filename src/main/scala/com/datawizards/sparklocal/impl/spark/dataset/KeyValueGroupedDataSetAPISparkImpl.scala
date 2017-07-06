package com.datawizards.sparklocal.impl.spark.dataset

import com.datawizards.sparklocal.dataset.agg.AggregationFunction
import com.datawizards.sparklocal.dataset.{DataSetAPI, KeyValueGroupedDataSetAPI}
import org.apache.spark.sql.{Encoder, KeyValueGroupedDataset}

import scala.reflect.ClassTag

class KeyValueGroupedDataSetAPISparkImpl[K: ClassTag, T: ClassTag](private[dataset] val data: KeyValueGroupedDataset[K, T]) extends KeyValueGroupedDataSetAPI[K, T] {
  private def create[U: ClassTag](data: KeyValueGroupedDataset[K,U]) = new KeyValueGroupedDataSetAPISparkImpl(data)

  override private[sparklocal] def toKeyValueGroupedDataSet(implicit encK: Encoder[K], encT: Encoder[T], encKT: Encoder[(K, T)]) = data

  override def count(): DataSetAPI[(K, Long)] =
    DataSetAPI(data.count())

  override def mapValues[W: ClassTag](func: (T) => W)
                                     (implicit enc: Encoder[W]=null): KeyValueGroupedDataSetAPI[K, W] =
    create(data.mapValues(func))

  override def mapGroups[U: ClassTag](f: (K, Iterator[T]) => U)
                                     (implicit enc: Encoder[U]=null): DataSetAPI[U] =
    DataSetAPI(data.mapGroups(f))

  override def reduceGroups(f: (T, T) => T): DataSetAPI[(K, T)] =
    DataSetAPI(data.reduceGroups(f))

  override def flatMapGroups[U: ClassTag](f: (K, Iterator[T]) => TraversableOnce[U])
                                         (implicit enc: Encoder[U]=null): DataSetAPI[U] =
    DataSetAPI(data.flatMapGroups(f))

  override def keys: DataSetAPI[K] =
    DataSetAPI(data.keys)

  override def cogroup[U: ClassTag, R: ClassTag](other: KeyValueGroupedDataSetAPI[K, U])
                                                (f: (K, Iterator[T], Iterator[U]) => TraversableOnce[R])
                                                (implicit
                                                 encK: Encoder[K]=null,
                                                 encT: Encoder[T]=null,
                                                 encU: Encoder[U]=null,
                                                 encR: Encoder[R]=null,
                                                 encKT: Encoder[(K,T)]=null,
                                                 encKU: Encoder[(K,U)]=null
                                                ): DataSetAPI[R] = {
    DataSetAPI(data.cogroup(other.toKeyValueGroupedDataSet)(f))
  }

  override def agg[U1: ClassTag](agg1: AggregationFunction[T, U1])
                                (implicit enc1: Encoder[U1]): DataSetAPI[(K, U1)] =
    DataSetAPI(data.agg(agg1.toTypedColumn))

  override def agg[U1: ClassTag, U2: ClassTag](
                                                agg1: AggregationFunction[T,U1],
                                                agg2: AggregationFunction[T,U2]
                                              )
                                              (implicit
                                               enc1: Encoder[U1],
                                               enc2: Encoder[U2],
                                               enc: Encoder[(U1,U2)]
                                              ): DataSetAPI[(K,U1,U2)] =
    DataSetAPI(data.agg(agg1.toTypedColumn, agg2.toTypedColumn))

  override def agg[U1: ClassTag, U2: ClassTag, U3: ClassTag](
                                                              agg1: AggregationFunction[T,U1],
                                                              agg2: AggregationFunction[T,U2],
                                                              agg3: AggregationFunction[T,U3]
                                                            )
                                                            (implicit
                                                             enc1: Encoder[U1],
                                                             enc2: Encoder[U2],
                                                             enc3: Encoder[U3],
                                                             enc: Encoder[(U1,U2,U3)]
                                                            ): DataSetAPI[(K,U1,U2,U3)] =
    DataSetAPI(data.agg(agg1.toTypedColumn, agg2.toTypedColumn, agg3.toTypedColumn))

  override def agg[U1: ClassTag, U2: ClassTag, U3: ClassTag, U4: ClassTag](
                                                                            agg1: AggregationFunction[T,U1],
                                                                            agg2: AggregationFunction[T,U2],
                                                                            agg3: AggregationFunction[T,U3],
                                                                            agg4: AggregationFunction[T,U4]
                                                                          )
                                                                          (implicit
                                                                           enc1: Encoder[U1],
                                                                           enc2: Encoder[U2],
                                                                           enc3: Encoder[U3],
                                                                           enc4: Encoder[U4],
                                                                           enc: Encoder[(U1,U2,U3,U4)]
                                                                          ): DataSetAPI[(K,U1,U2,U3,U4)] =
    DataSetAPI(data.agg(agg1.toTypedColumn, agg2.toTypedColumn, agg3.toTypedColumn, agg4.toTypedColumn))

}
