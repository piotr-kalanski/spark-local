package com.datawizards.sparklocal.dataset

import org.apache.spark.sql.{Encoder, KeyValueGroupedDataset}

import scala.reflect.ClassTag

class KeyValueGroupedDataSetAPIScalaImpl[K: ClassTag, T: ClassTag](private[dataset] val data: Map[K,Seq[T]]) extends KeyValueGroupedDataSetAPI[K, T] {

  private def create[U: ClassTag](data: Map[K,Seq[U]]) = new KeyValueGroupedDataSetAPIScalaImpl(data)

  private def createKeyValueGroupedDataset(d: Map[K,Seq[T]])
                                          (implicit encK: Encoder[K], encT: Encoder[T], encKT: Encoder[(K, T)]): KeyValueGroupedDataset[K,T] = {
    val ds = spark.createDataset(d.toSeq.flatMap(kv => kv._2.map(v => (kv._1,v))))
    ds
      .groupByKey(_._1)
      .mapValues(x => x._2)
  }

  override private[dataset] def toKeyValueGroupedDataSet(implicit encK: Encoder[K], encT: Encoder[T], encKT: Encoder[(K, T)]) = createKeyValueGroupedDataset(data)

  override def count(): DataSetAPI[(K, Long)] =
    DataSetAPI(data.mapValues(_.size.toLong))

  override def mapValues[W: ClassTag](func: (T) => W)
                                     (implicit enc: Encoder[W]=null): KeyValueGroupedDataSetAPI[K, W] =
    create(data.mapValues(_.map(func)))

  override def mapGroups[U: ClassTag](f: (K, Iterator[T]) => U)
                                     (implicit enc: Encoder[U]=null): DataSetAPI[U] =
    DataSetAPI(data.map{case (k, values) => f(k,values.iterator)})

  override def reduceGroups(f: (T, T) => T): DataSetAPI[(K, T)] =
    DataSetAPI(data.mapValues(_.reduce(f)))

  override def flatMapGroups[U: ClassTag](f: (K, Iterator[T]) => TraversableOnce[U])
                                         (implicit enc: Encoder[U]=null): DataSetAPI[U] =
    DataSetAPI(
      data
        .flatMap{case (k,vals) => f(k, vals.iterator)}
    )

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
                                                 ): DataSetAPI[R] = other match {
    case sparkKV:KeyValueGroupedDataSetAPISparkImpl[K,U] => new KeyValueGroupedDataSetAPISparkImpl(this.toKeyValueGroupedDataSet).cogroup(sparkKV)(f)
    case scalaKV:KeyValueGroupedDataSetAPIScalaImpl[K,U] => DataSetAPI({
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

}
