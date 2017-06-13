package com.datawizards.sparklocal.impl.scala.`lazy`.dataset

import com.datawizards.sparklocal.dataset.KeyValueGroupedDataSetAPI
import com.datawizards.sparklocal.impl.scala.dataset.{DataSetAPIScalaBase, KeyValueGroupedDataSetAPIScalaBase}
import org.apache.spark.sql.Encoder

import scala.collection.{GenIterable, SeqView}
import scala.reflect.ClassTag

class KeyValueGroupedDataSetAPIScalaLazyImpl[K: ClassTag, T: ClassTag](private[sparklocal] val data: Map[K,SeqView[T, Seq[T]]]) extends KeyValueGroupedDataSetAPIScalaBase[K, T] {
  override type InternalCollection = Map[K, SeqView[T, Seq[T]]]

  override private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]=null): DataSetAPIScalaBase[U] =
    DataSetAPIScalaLazyImpl.create(it)

  override def mapValues[W: ClassTag](func: (T) => W)
                                     (implicit enc: Encoder[W]=null): KeyValueGroupedDataSetAPI[K, W] =
    new KeyValueGroupedDataSetAPIScalaLazyImpl(data.mapValues(_.map(func).view))
}
