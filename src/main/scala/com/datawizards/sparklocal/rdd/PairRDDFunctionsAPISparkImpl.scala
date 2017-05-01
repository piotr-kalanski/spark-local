package com.datawizards.sparklocal.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.collection.Map
import scala.reflect.ClassTag

class PairRDDFunctionsAPISparkImpl[K, V](rdd: RDDAPISparkImpl[(K,V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctionsAPI[K,V]
{
  private val data: PairRDDFunctions[K,V] = RDD.rddToPairRDDFunctions(rdd.data)(kt, vt, ord)

  override def mapValues[U](f: (V) => U): RDDAPI[(K, U)] = RDDAPI(data.mapValues(f))

  override def keys: RDDAPI[K] = RDDAPI(data.keys)

  override def values: RDDAPI[V] = RDDAPI(data.values)

  override def flatMapValues[U](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)] = RDDAPI(data.flatMapValues(f))

  override def countByKey(): Map[K, Long] = data.countByKey()

  override def reduceByKey(func: (V, V) => V): RDDAPI[(K, V)] = RDDAPI(data.reduceByKey(func))

  override def reduceByKey(func: (V, V) => V, numPartitions: Int): RDDAPI[(K, V)] = RDDAPI(data.reduceByKey(func, numPartitions))

  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDDAPI[(K, V)] = RDDAPI(data.reduceByKey(partitioner, func))

  override def reduceByKeyLocally(func: (V, V) => V): Map[K, V] = data.reduceByKeyLocally(func)

  override def groupByKey(): RDDAPI[(K, Iterable[V])] = RDDAPI(data.groupByKey())

  override def groupByKey(numPartitions: Int): RDDAPI[(K, Iterable[V])] = RDDAPI(data.groupByKey(numPartitions))

  override def groupByKey(partitioner: Partitioner): RDDAPI[(K, Iterable[V])] = RDDAPI(data.groupByKey(partitioner))

  override def foldByKey(zeroValue: V)(func: (V, V) => V): RDDAPI[(K, V)] = RDDAPI(data.foldByKey(zeroValue)(func))

  override def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDDAPI[(K, V)] = RDDAPI(data.foldByKey(zeroValue, numPartitions)(func))

  override def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDDAPI[(K, V)] = RDDAPI(data.foldByKey(zeroValue, partitioner)(func))
}
