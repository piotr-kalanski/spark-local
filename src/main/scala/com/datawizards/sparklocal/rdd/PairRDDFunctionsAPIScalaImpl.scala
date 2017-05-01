package com.datawizards.sparklocal.rdd

import org.apache.spark.Partitioner

import scala.reflect.ClassTag
import scala.collection.Map

class PairRDDFunctionsAPIScalaImpl[K,V](rdd: RDDAPIScalaImpl[(K,V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctionsAPI[K,V]
{
  private val data = rdd.data

  override def mapValues[U](f: (V) => U): RDDAPI[(K, U)] =
    RDDAPI(
      data.map { case (k, v) => (k, f(v)) }
    )

  override def keys: RDDAPI[K] = rdd.map(_._1)

  override def values: RDDAPI[V] = rdd.map(_._2)

  override def flatMapValues[U](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)] =
    rdd.flatMap { case (k,v) =>
      f(v).map(x => (k,x))
    }

  override def countByKey(): Map[K, Long] =
    data
      .groupBy(_._1)
      .mapValues(_.size)

  override def reduceByKey(func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(
      data
        .groupBy(_._1)
        .mapValues(
          _.map(_._2)
            .reduce(func)
        )
    )

  override def reduceByKey(func: (V, V) => V, numPartitions: Int): RDDAPI[(K, V)] = reduceByKey(func)

  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDDAPI[(K, V)] = reduceByKey(func)

  override def reduceByKeyLocally(func: (V, V) => V): Map[K, V] =
    reduceByKey(func).collect().toMap

  override def groupByKey(): RDDAPI[(K, Iterable[V])] =
    RDDAPI(
      data
        .groupBy(_._1)
        .mapValues(_.map(_._2))
    )

  override def groupByKey(numPartitions: Int): RDDAPI[(K, Iterable[V])] = groupByKey()

  override def groupByKey(partitioner: Partitioner): RDDAPI[(K, Iterable[V])] = groupByKey()

  override def foldByKey(zeroValue: V)(func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(
      data
        .groupBy(_._1)
        .mapValues(
          _.map(_._2)
            .fold(zeroValue)(func)
        )
    )

  override def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDDAPI[(K, V)] = foldByKey(zeroValue)(func)

  override def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDDAPI[(K, V)] = foldByKey(zeroValue)(func)
}
