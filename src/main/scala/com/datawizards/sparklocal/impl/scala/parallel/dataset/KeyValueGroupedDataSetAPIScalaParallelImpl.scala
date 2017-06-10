package com.datawizards.sparklocal.impl.scala.parallel.dataset

import com.datawizards.sparklocal.dataset.KeyValueGroupedDataSetAPI
import com.datawizards.sparklocal.impl.scala.dataset.{DataSetAPIScalaBase, KeyValueGroupedDataSetAPIScalaBase}
import org.apache.spark.sql.Encoder

import scala.collection.GenIterable
import scala.collection.parallel.{ParMap, ParSeq}
import scala.reflect.ClassTag

class KeyValueGroupedDataSetAPIScalaParallelImpl[K: ClassTag, T: ClassTag](private[sparklocal] val data: ParMap[K,ParSeq[T]]) extends KeyValueGroupedDataSetAPIScalaBase[K, T] {
  override type InternalCollection = ParMap[K, ParSeq[T]]

  override private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]=null): DataSetAPIScalaBase[U] =
    DataSetAPIScalaParallelImpl.create(it)

  override def mapValues[W: ClassTag](func: (T) => W)
                                     (implicit enc: Encoder[W]=null): KeyValueGroupedDataSetAPI[K, W] =
    new KeyValueGroupedDataSetAPIScalaParallelImpl(data.mapValues(_.map(func).par))
}
