package com.datawizards.sparklocal.impl.spark.rdd

import com.datawizards.sparklocal.rdd.{PairRDDFunctionsAPI, RDDAPI}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.collection.{GenIterable, GenMap, Map}
import scala.reflect.ClassTag

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

  override def countByKey(): GenMap[K, Long] =
    data.countByKey()

  override def reduceByKey(func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(data.reduceByKey(func))

  override def reduceByKey(func: (V, V) => V, numPartitions: Int): RDDAPI[(K, V)] =
    RDDAPI(data.reduceByKey(func, numPartitions))

  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(data.reduceByKey(partitioner, func))

  override def reduceByKeyLocally(func: (V, V) => V): Map[K, V] =
    data.reduceByKeyLocally(func)

  override def groupByKey(): RDDAPI[(K, GenIterable[V])] =
    RDDAPI(data.groupByKey().mapValues(i => i.asInstanceOf[GenIterable[V]]))

  override def groupByKey(numPartitions: Int): RDDAPI[(K, GenIterable[V])] =
    RDDAPI(data.groupByKey(numPartitions).mapValues(i => i.asInstanceOf[GenIterable[V]]))

  override def groupByKey(partitioner: Partitioner): RDDAPI[(K, GenIterable[V])] =
    RDDAPI(data.groupByKey(partitioner).mapValues(i => i.asInstanceOf[GenIterable[V]]))

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

  override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], partitioner: Partitioner): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2], GenIterable[W3]))] =
    RDDAPI(
      data
        .cogroup(other1.toRDD, other2.toRDD, other3.toRDD, partitioner)
        .mapValues(v => (
          v._1.asInstanceOf[GenIterable[V]],
          v._2.asInstanceOf[GenIterable[W1]],
          v._3.asInstanceOf[GenIterable[W2]],
          v._4.asInstanceOf[GenIterable[W3]]
        ))
    )

  override def cogroup[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (GenIterable[V], GenIterable[W]))] =
    RDDAPI(
      data
        .cogroup(other.toRDD, partitioner)
        .mapValues(v => (
          v._1.asInstanceOf[GenIterable[V]],
          v._2.asInstanceOf[GenIterable[W]]
        ))
    )

  override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], partitioner: Partitioner): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2]))] =
    RDDAPI(
      data
        .cogroup(other1.toRDD, other2.toRDD, partitioner)
        .mapValues(v => (
          v._1.asInstanceOf[GenIterable[V]],
          v._2.asInstanceOf[GenIterable[W1]],
          v._3.asInstanceOf[GenIterable[W2]]
        ))
    )

  override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)]): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2], GenIterable[W3]))] =
    RDDAPI(
      data
        .cogroup(other1.toRDD, other2.toRDD, other3.toRDD)
        .mapValues(v => (
          v._1.asInstanceOf[GenIterable[V]],
          v._2.asInstanceOf[GenIterable[W1]],
          v._3.asInstanceOf[GenIterable[W2]],
          v._4.asInstanceOf[GenIterable[W3]]
        ))
    )

  override def cogroup[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (GenIterable[V], GenIterable[W]))] =
    RDDAPI(
      data
        .cogroup(other.toRDD)
        .mapValues(v => (
          v._1.asInstanceOf[GenIterable[V]],
          v._2.asInstanceOf[GenIterable[W]]
        ))
    )

  override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)]): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2]))] =
    RDDAPI(
      data
        .cogroup(other1.toRDD, other2.toRDD)
        .mapValues(v => (
          v._1.asInstanceOf[GenIterable[V]],
          v._2.asInstanceOf[GenIterable[W1]],
          v._3.asInstanceOf[GenIterable[W2]]
        ))
    )

  override def cogroup[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (GenIterable[V], GenIterable[W]))] =
    RDDAPI(
      data
        .cogroup(other.toRDD, numPartitions)
        .mapValues(v => (
          v._1.asInstanceOf[GenIterable[V]],
          v._2.asInstanceOf[GenIterable[W]]
        ))
    )

  override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], numPartitions: Int): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2]))] =
    RDDAPI(
      data
        .cogroup(other1.toRDD, other2.toRDD, numPartitions)
        .mapValues(v => (
          v._1.asInstanceOf[GenIterable[V]],
          v._2.asInstanceOf[GenIterable[W1]],
          v._3.asInstanceOf[GenIterable[W2]]
        ))
    )

  override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], numPartitions: Int): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2], GenIterable[W3]))] =
    RDDAPI(
      data
        .cogroup(other1.toRDD, other2.toRDD, other3.toRDD, numPartitions)
        .mapValues(v => (
          v._1.asInstanceOf[GenIterable[V]],
          v._2.asInstanceOf[GenIterable[W1]],
          v._3.asInstanceOf[GenIterable[W2]],
          v._4.asInstanceOf[GenIterable[W3]]
        ))
    )

  override def collectAsMap(): GenMap[K, V] =
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
