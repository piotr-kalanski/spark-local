package com.datawizards.sparklocal.rdd

trait PairRDDFunctionsAPI[K, V] {
  def mapValues[U](f: (V) => U): RDDAPI[(K, U)]
  def keys: RDDAPI[K]
  def values: RDDAPI[V]
}
