package com.datawizards.sparklocal.rdd

import scala.reflect.ClassTag

class PairRDDFunctionsAPIScalaImpl[K,V](rdd: RDDAPIScalaImpl[(K,V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctionsAPI[K,V]
{
  private val data = rdd.data

  override def mapValues[U](f: (V) => U): RDDAPI[(K, U)] =
    RDDAPI(data.map { case (k, v) => (k, f(v)) })

  override def keys: RDDAPI[K] = rdd.map(_._1)

  override def values: RDDAPI[V] = rdd.map(_._2)

  override def flatMapValues[U](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)] =
    rdd.flatMap{ case(k,v) => f(v).map(x => (k,x))}
}
