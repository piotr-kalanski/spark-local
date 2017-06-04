package com.datawizards.sparklocal.impl.spark.dataset

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


}