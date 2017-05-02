package com.datawizards.sparklocal.dataset

import scala.reflect.runtime.universe.TypeTag

class KeyValueGroupedDataSetAPIScalaImpl[K: TypeTag, T](data: Map[K,Seq[T]]) extends KeyValueGroupedDataSetAPI[K, T] {
  override def count(): DataSetAPI[(K, Long)] =
    DataSetAPI(data.mapValues(_.size.toLong))
}
