package com.datawizards.sparklocal.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.{GenIterable, GenMap, Map}
import scala.reflect.ClassTag

trait PairRDDFunctionsAPI[K, V] {
  protected lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  protected def parallelize[That: ClassTag](d: Seq[That]): RDD[That] = spark.sparkContext.parallelize(d)
  protected def parallelize[That: ClassTag](d: GenIterable[That]): RDD[That] = parallelize(d.toList)

  def mapValues[U: ClassTag](f: (V) => U): RDDAPI[(K, U)]
  def keys: RDDAPI[K]
  def values: RDDAPI[V]
  def flatMapValues[U: ClassTag](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)]
  def countByKey(): GenMap[K, Long]
  def reduceByKey(func: (V, V) => V): RDDAPI[(K, V)]
  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDDAPI[(K, V)]
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDDAPI[(K, V)]
  def reduceByKeyLocally(func: (V, V) => V): Map[K, V]
  def groupByKey(): RDDAPI[(K, GenIterable[V])]
  def groupByKey(numPartitions: Int): RDDAPI[(K, GenIterable[V])]
  def groupByKey(partitioner: Partitioner): RDDAPI[(K, GenIterable[V])]
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDDAPI[(K, V)]
  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDDAPI[(K, V)]
  def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDDAPI[(K, V)]
  def join[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, W))]
  def join[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, W))]
  def join[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, W))]
  def leftOuterJoin[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, Option[W]))]
  def leftOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, Option[W]))]
  def leftOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, Option[W]))]
  def rightOuterJoin[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], W))]
  def rightOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], W))]
  def rightOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], W))]
  def fullOuterJoin[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], Option[W]))]
  def fullOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], Option[W]))]
  def fullOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], Option[W]))]
  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], partitioner: Partitioner): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2], GenIterable[W3]))]
  def cogroup[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (GenIterable[V], GenIterable[W]))]
  def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], partitioner: Partitioner): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2]))]
  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)]): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2], GenIterable[W3]))]
  def cogroup[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (GenIterable[V], GenIterable[W]))]
  def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)]): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2]))]
  def cogroup[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (GenIterable[V], GenIterable[W]))]
  def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], numPartitions: Int): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2]))]
  def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], numPartitions: Int): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2], GenIterable[W3]))]
  def collectAsMap(): GenMap[K, V]
  def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, V)]
  def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, V)]
  def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], p: Partitioner): RDDAPI[(K, V)]
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)]
  def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)]
  def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)]
  def partitionBy(partitioner: Partitioner): RDDAPI[(K, V)]

}
