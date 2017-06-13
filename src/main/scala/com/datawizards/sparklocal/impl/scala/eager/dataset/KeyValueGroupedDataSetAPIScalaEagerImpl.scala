package com.datawizards.sparklocal.impl.scala.eager.dataset

import com.datawizards.sparklocal.dataset.KeyValueGroupedDataSetAPI
import com.datawizards.sparklocal.impl.scala.dataset.{DataSetAPIScalaBase, KeyValueGroupedDataSetAPIScalaBase}
import org.apache.spark.sql.Encoder

import scala.collection.GenIterable
import scala.reflect.ClassTag

class KeyValueGroupedDataSetAPIScalaEagerImpl[K: ClassTag, T: ClassTag](private[sparklocal] val data: Map[K,Seq[T]]) extends KeyValueGroupedDataSetAPIScalaBase[K, T] {
  override type InternalCollection = Map[K, Seq[T]]

  override private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]=null): DataSetAPIScalaBase[U] =
    DataSetAPIScalaEagerImpl.create(it)

  override def mapValues[W: ClassTag](func: (T) => W)
                                     (implicit enc: Encoder[W]=null): KeyValueGroupedDataSetAPI[K, W] =
    new KeyValueGroupedDataSetAPIScalaEagerImpl(data.mapValues(_.map(func)))
}
