package com.datawizards.sparklocal.impl.scala.parallellazy.dataset

import com.datawizards.sparklocal.dataset.KeyValueGroupedDataSetAPI
import com.datawizards.sparklocal.impl.scala.dataset.{DataSetAPIScalaBase, KeyValueGroupedDataSetAPIScalaBase}
import org.apache.spark.sql.Encoder

import scala.collection.{GenIterable, GenSeq}
import scala.reflect.ClassTag

class KeyValueGroupedDataSetAPIScalaParallelLazyImpl[K: ClassTag, T: ClassTag](private[sparklocal] val data: Map[K, GenSeq[T]]) extends KeyValueGroupedDataSetAPIScalaBase[K, T] {
  override type InternalCollection = Map[K, GenSeq[T]]

  override private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]=null): DataSetAPIScalaBase[U] =
    DataSetAPIScalaParallelLazyImpl.create(it)

  override def mapValues[W: ClassTag](func: (T) => W)
                                     (implicit enc: Encoder[W]=null): KeyValueGroupedDataSetAPI[K, W] = {
    val mapped = data.mapValues(_.map(func))
    new KeyValueGroupedDataSetAPIScalaParallelLazyImpl(mapped)
  }

}
