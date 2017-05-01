package com.datawizards.sparklocal.rdd

import org.apache.spark.Partitioner

import scala.collection.Map

trait PairRDDFunctionsAPI[K, V] {
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
}
