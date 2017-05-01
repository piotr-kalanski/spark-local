package com.datawizards.sparklocal.rdd

import scala.collection.Map

trait PairRDDFunctionsAPI[K, V] {
  def mapValues[U](f: (V) => U): RDDAPI[(K, U)]
  def keys: RDDAPI[K]
  def values: RDDAPI[V]
  def flatMapValues[U](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)]
  def countByKey(): Map[K, Long]
  def reduceByKey(func: (V, V) ⇒ V): RDDAPI[(K, V)]
}
