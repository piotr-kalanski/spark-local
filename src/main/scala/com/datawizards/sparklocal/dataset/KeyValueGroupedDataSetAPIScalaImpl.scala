package com.datawizards.sparklocal.dataset

import org.apache.spark.sql.KeyValueGroupedDataset
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class KeyValueGroupedDataSetAPIScalaImpl[K: ClassTag: TypeTag, T: ClassTag: TypeTag](private[dataset] val data: Map[K,Seq[T]]) extends KeyValueGroupedDataSetAPI[K, T] {

  private def create[U: ClassTag: TypeTag](data: Map[K,Seq[U]]) = new KeyValueGroupedDataSetAPIScalaImpl(data)

  private def createKeyValueGroupedDataset(d: Map[K,Seq[T]]): KeyValueGroupedDataset[K,T] = {
    implicit val keyEncoder = ExpressionEncoder[K]()
    implicit val valuesEncoder = ExpressionEncoder[T]()
    implicit val encoder = ExpressionEncoder[(K,T)]()
    val ds = spark.createDataset(d.toSeq.flatMap(kv => kv._2.map(v => (kv._1,v))))
    ds
      .groupByKey(_._1)
      .mapValues(x => x._2)
  }

  override private[dataset] def toKeyValueGroupedDataSet = createKeyValueGroupedDataset(data)

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

  override def keys: DataSetAPI[K] =
    DataSetAPI(data.keys)

  override def cogroup[U: ClassTag : TypeTag, R: ClassTag : TypeTag](other: KeyValueGroupedDataSetAPI[K, U])(f: (K, Iterator[T], Iterator[U]) => TraversableOnce[R]): DataSetAPI[R] = other match {
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
