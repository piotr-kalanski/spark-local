package com.datawizards.sparklocal.impl.scala.parallellazy.rdd

import com.datawizards.sparklocal.impl.scala.rdd.{PairRDDFunctionsAPIScalaBase, RDDAPIScalaBase}

import scala.collection.GenIterable
import scala.reflect.ClassTag

class PairRDDFunctionsAPIScalaParallelLazyImpl[K, V](protected val rdd: RDDAPIScalaParallelLazyImpl[(K,V)])
                                                    (implicit kct: ClassTag[K], vct: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctionsAPIScalaBase[K,V] {

  override protected def create[U: ClassTag](data: GenIterable[U]): RDDAPIScalaBase[U] =
    rdd.create(data)

}
