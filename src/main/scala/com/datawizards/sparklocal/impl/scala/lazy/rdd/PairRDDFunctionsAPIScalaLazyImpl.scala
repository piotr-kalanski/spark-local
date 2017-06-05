package com.datawizards.sparklocal.impl.scala.`lazy`.rdd

import com.datawizards.sparklocal.impl.scala.rdd.PairRDDFunctionsAPIScalaBase

import scala.reflect.ClassTag

class PairRDDFunctionsAPIScalaLazyImpl[K, V](protected val rdd: RDDAPIScalaLazyImpl[(K,V)])
                                            (implicit kct: ClassTag[K], vct: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctionsAPIScalaBase[K,V]
