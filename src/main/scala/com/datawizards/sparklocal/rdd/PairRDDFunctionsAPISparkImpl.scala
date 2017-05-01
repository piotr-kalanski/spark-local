package com.datawizards.sparklocal.rdd

import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.ClassTag

class PairRDDFunctionsAPISparkImpl[K, V](rdd: RDDAPISparkImpl[(K,V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctionsAPI[K,V]
{
  private val data: PairRDDFunctions[K,V] = RDD.rddToPairRDDFunctions(rdd.data)(kt, vt, ord)

  override def mapValues[U](f: (V) => U): RDDAPI[(K, U)] = RDDAPI(data.mapValues(f))

  override def keys: RDDAPI[K] = RDDAPI(data.keys)

  override def values: RDDAPI[V] = RDDAPI(data.values)

  override def flatMapValues[U](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)] = RDDAPI(data.flatMapValues(f))
}
