package com.datawizards.sparklocal.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.collection.Map
import scala.reflect.ClassTag
//import scala.reflect.runtime.universe.TypeTag

class PairRDDFunctionsAPISparkImpl[K, V](rdd: RDDAPISparkImpl[(K,V)])
  (implicit kct: ClassTag[K], vct: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctionsAPI[K,V]
{
  private val data: PairRDDFunctions[K,V] =
    RDD.rddToPairRDDFunctions(rdd.data)(implicitly[ClassTag[K]], implicitly[ClassTag[V]], ord)

  override def mapValues[U: ClassTag](f: (V) => U): RDDAPI[(K, U)] =
    RDDAPI(data.mapValues(f))

  override def keys: RDDAPI[K] =
    RDDAPI(data.keys)

  override def values: RDDAPI[V] =
    RDDAPI(data.values)

  override def flatMapValues[U: ClassTag](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)] =
    RDDAPI(data.flatMapValues(f))

  override def countByKey(): Map[K, Long] =
    data.countByKey()

  override def reduceByKey(func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(data.reduceByKey(func))

  override def reduceByKey(func: (V, V) => V, numPartitions: Int): RDDAPI[(K, V)] =
    RDDAPI(data.reduceByKey(func, numPartitions))

  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(data.reduceByKey(partitioner, func))

  override def reduceByKeyLocally(func: (V, V) => V): Map[K, V] =
    data.reduceByKeyLocally(func)

  override def groupByKey(): RDDAPI[(K, Iterable[V])] =
    RDDAPI(data.groupByKey())

  override def groupByKey(numPartitions: Int): RDDAPI[(K, Iterable[V])] =
    RDDAPI(data.groupByKey(numPartitions))

  override def groupByKey(partitioner: Partitioner): RDDAPI[(K, Iterable[V])] =
    RDDAPI(data.groupByKey(partitioner))

  override def foldByKey(zeroValue: V)(func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(data.foldByKey(zeroValue)(func))

  override def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(data.foldByKey(zeroValue, numPartitions)(func))

  override def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(data.foldByKey(zeroValue, partitioner)(func))

  override def join[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, W))] =
    RDDAPI(data.join(other.toRDD))

  override def join[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, W))] =
    RDDAPI(data.join(other.toRDD, numPartitions))

  override def join[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, W))] =
    RDDAPI(data.join(other.toRDD, partitioner))

  override def leftOuterJoin[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, Option[W]))] =
    RDDAPI(data.leftOuterJoin(other.toRDD))

  override def leftOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, Option[W]))] =
    RDDAPI(data.leftOuterJoin(other.toRDD, numPartitions))

  override def leftOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, Option[W]))] =
    RDDAPI(data.leftOuterJoin(other.toRDD, partitioner))

  override def rightOuterJoin[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], W))] =
    RDDAPI(data.rightOuterJoin(other.toRDD))

  override def rightOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], W))] =
    RDDAPI(data.rightOuterJoin(other.toRDD, numPartitions))

  override def rightOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], W))] =
    RDDAPI(data.rightOuterJoin(other.toRDD, partitioner))

  override def fullOuterJoin[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], Option[W]))] =
    RDDAPI(data.fullOuterJoin(other.toRDD))

  override def fullOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], Option[W]))] =
    RDDAPI(data.fullOuterJoin(other.toRDD, numPartitions))

  override def fullOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], Option[W]))] =
    RDDAPI(data.fullOuterJoin(other.toRDD, partitioner))

  override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], partitioner: Partitioner): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    RDDAPI(data.cogroup(other1.toRDD, other2.toRDD, other3.toRDD, partitioner))

  override def cogroup[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Iterable[V], Iterable[W]))] =
    RDDAPI(data.cogroup(other.toRDD, partitioner))

  override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], partitioner: Partitioner): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    RDDAPI(data.cogroup(other1.toRDD, other2.toRDD, partitioner))

  override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)]): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    RDDAPI(data.cogroup(other1.toRDD, other2.toRDD, other3.toRDD))

  override def cogroup[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (Iterable[V], Iterable[W]))] =
    RDDAPI(data.cogroup(other.toRDD))

  override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)]): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    RDDAPI(data.cogroup(other1.toRDD, other2.toRDD))

  override def cogroup[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Iterable[V], Iterable[W]))] =
    RDDAPI(data.cogroup(other.toRDD, numPartitions))

  override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], numPartitions: Int): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    RDDAPI(data.cogroup(other1.toRDD, other2.toRDD, numPartitions))

  override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], numPartitions: Int): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    RDDAPI(data.cogroup(other1.toRDD, other2.toRDD, other3.toRDD, numPartitions))

  override def collectAsMap(): Map[K, V] =
    data.collectAsMap()

  override def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, V)] =
    RDDAPI(data.subtractByKey(other.toRDD))

  override def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, V)] =
    RDDAPI(data.subtractByKey(other.toRDD, numPartitions))

  override def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], p: Partitioner): RDDAPI[(K, V)] =
    RDDAPI(data.subtractByKey(other.toRDD, p))

  override def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)] =
    RDDAPI(data.aggregateByKey(zeroValue)(seqOp, combOp))

  override def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)] =
    RDDAPI(data.aggregateByKey(zeroValue, partitioner)(seqOp, combOp))

  override def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)] =
    RDDAPI(data.aggregateByKey(zeroValue, numPartitions)(seqOp, combOp))

  override def partitionBy(partitioner: Partitioner): RDDAPI[(K, V)] =
    RDDAPI(data.partitionBy(partitioner))

}
