package com.datawizards.sparklocal.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map
import scala.reflect.ClassTag

trait PairRDDFunctionsAPI[K, V] {
  protected lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  protected def parallelize[That: ClassTag](d: Seq[That]): RDD[That] = spark.sparkContext.parallelize(d)

  def mapValues[U](f: (V) => U): RDDAPI[(K, U)]
  def keys: RDDAPI[K]
  def values: RDDAPI[V]
  def flatMapValues[U](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)]
  def countByKey(): Map[K, Long]
  def reduceByKey(func: (V, V) => V): RDDAPI[(K, V)]
  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDDAPI[(K, V)]
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDDAPI[(K, V)]
  def reduceByKeyLocally(func: (V, V) => V): Map[K, V]
  def groupByKey(): RDDAPI[(K, Iterable[V])]
  def groupByKey(numPartitions: Int): RDDAPI[(K, Iterable[V])]
  def groupByKey(partitioner: Partitioner): RDDAPI[(K, Iterable[V])]
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDDAPI[(K, V)]
  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDDAPI[(K, V)]
  def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDDAPI[(K, V)]
  def join[W](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, W))]
  def join[W](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, W))]
  def join[W](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, W))]
  def leftOuterJoin[W](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, Option[W]))]
  def leftOuterJoin[W](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, Option[W]))]
  def leftOuterJoin[W](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, Option[W]))]
  def rightOuterJoin[W](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], W))]
  def rightOuterJoin[W](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], W))]
  def rightOuterJoin[W](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], W))]
  def fullOuterJoin[W](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], Option[W]))]
  def fullOuterJoin[W](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], Option[W]))]
  def fullOuterJoin[W](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], Option[W]))]
  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], partitioner: Partitioner): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]
  def cogroup[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Iterable[V], Iterable[W]))]
  def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], partitioner: Partitioner): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)]): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]
  def cogroup[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (Iterable[V], Iterable[W]))]
  def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)]): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
  def cogroup[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Iterable[V], Iterable[W]))]
  def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], numPartitions: Int): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], numPartitions: Int): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))]
  def collectAsMap(): Map[K, V]
  def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, V)]
  def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, V)]
  def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], p: Partitioner): RDDAPI[(K, V)]
}
