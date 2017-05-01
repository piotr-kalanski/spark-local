package com.datawizards.sparklocal.rdd

class PairRDDFunctionsAPIScalaImpl[K,V](rdd: RDDAPIScalaImpl[(K,V)]) extends PairRDDFunctionsAPI[K,V] {
  private val data = rdd.data

  override def mapValues[U](f: (V) => U): RDDAPI[(K, U)] =
    RDDAPI(data.map { case (k, v) => (k, f(v)) })
}
