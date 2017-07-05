package com.datawizards.sparklocal.impl.scala.dataset

import com.datawizards.sparklocal.dataset.agg.AggregationFunction
import com.datawizards.sparklocal.dataset.{DataSetAPI, KeyValueGroupedDataSetAPI}
import com.datawizards.sparklocal.impl.spark.dataset.KeyValueGroupedDataSetAPISparkImpl
import org.apache.spark.sql.{Encoder, KeyValueGroupedDataset}

import scala.collection.{GenIterable, GenMap}
import scala.reflect.ClassTag

abstract class KeyValueGroupedDataSetAPIScalaBase[K: ClassTag, T: ClassTag] extends KeyValueGroupedDataSetAPI[K, T] {
  type InternalCollection <: GenMap[K, GenIterable[T]]

  private[sparklocal] val data: InternalCollection

  private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]=null): DataSetAPIScalaBase[U]

  private def createKeyValueGroupedDataset(d: InternalCollection)
                                          (implicit encK: Encoder[K], encT: Encoder[T], encKT: Encoder[(K, T)]): KeyValueGroupedDataset[K,T] = {
    val seqData = d.toSeq.seq.flatMap(kv => kv._2.map(v => (kv._1,v)))
    val ds = spark.createDataset(seqData)
    ds
      .groupByKey(_._1)
      .mapValues(x => x._2)
  }

  override private[sparklocal] def toKeyValueGroupedDataSet(implicit encK: Encoder[K], encT: Encoder[T], encKT: Encoder[(K, T)]) =
    createKeyValueGroupedDataset(data)

  override def count(): DataSetAPI[(K, Long)] =
    create(data.mapValues(_.size.toLong))

  override def mapGroups[U: ClassTag](f: (K, Iterator[T]) => U)
                                     (implicit enc: Encoder[U]=null): DataSetAPI[U] =
    create(data.map{case (k, values) => f(k,values.iterator)})

  override def reduceGroups(f: (T, T) => T): DataSetAPI[(K, T)] =
    create(data.mapValues(_.reduce(f)))

  override def flatMapGroups[U: ClassTag](f: (K, Iterator[T]) => TraversableOnce[U])
                                         (implicit enc: Encoder[U]=null): DataSetAPI[U] =
    create(
      data
        .flatMap{case (k,vals) => f(k, vals.iterator)}
    )

  override def keys: DataSetAPI[K] =
    create(data.keys)

  override def cogroup[U: ClassTag, R: ClassTag](other: KeyValueGroupedDataSetAPI[K, U])
                                                 (f: (K, Iterator[T], Iterator[U]) => TraversableOnce[R])
                                                 (implicit
                                                  encK: Encoder[K]=null,
                                                  encT: Encoder[T]=null,
                                                  encU: Encoder[U]=null,
                                                  encR: Encoder[R]=null,
                                                  encKT: Encoder[(K,T)]=null,
                                                  encKU: Encoder[(K,U)]=null
                                                 ): DataSetAPI[R] = other match {
    case sparkKV:KeyValueGroupedDataSetAPISparkImpl[K,U] => new KeyValueGroupedDataSetAPISparkImpl(this.toKeyValueGroupedDataSet).cogroup(sparkKV)(f)
    case scalaKV:KeyValueGroupedDataSetAPIScalaBase[K,U] => create({
      val xs      = data
      val ys      = scalaKV.data
      val allKeys = xs.keys ++ ys.keys
      allKeys.flatMap { key =>
        val xsWithKey = xs.getOrElse(key, Iterable.empty)
        val ysWithKey = ys.getOrElse(key, Iterable.empty)
        f(key, xsWithKey.iterator, ysWithKey.iterator)
      }
    })
  }

  override def agg[U1: ClassTag](agg1: AggregationFunction[T, U1])
                                (implicit enc1: Encoder[U1]): DataSetAPI[(K, U1)] =
    create(
      data.mapValues(values => agg1.aggregate(values))
    )

  override def agg[U1: ClassTag, U2: ClassTag](agg1: AggregationFunction[T, U1], agg2: AggregationFunction[T, U2])
                                              (implicit enc1: Encoder[U1], enc2: Encoder[U2], enc: Encoder[(U1, U2)]): DataSetAPI[(K, U1, U2)] =
    create(
      data
        .mapValues(values => (agg1.aggregate(values), agg2.aggregate(values)))
        .map{case (k,(a1,a2)) => (k,a1,a2)}
    )
}
