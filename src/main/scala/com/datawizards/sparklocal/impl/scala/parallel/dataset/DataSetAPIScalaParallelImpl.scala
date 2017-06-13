package com.datawizards.sparklocal.impl.scala.parallel.dataset

import com.datawizards.sparklocal.dataset.{DataSetAPI, KeyValueGroupedDataSetAPI}
import com.datawizards.sparklocal.impl.scala.`lazy`.dataset.DataSetAPIScalaLazyImpl
import com.datawizards.sparklocal.impl.scala.dataset.DataSetAPIScalaBase
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.Encoder

import scala.collection.GenIterable
import scala.collection.parallel.ParSeq
import scala.reflect.ClassTag

object DataSetAPIScalaParallelImpl {
  private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]): DataSetAPIScalaBase[U] =
    new DataSetAPIScalaParallelImpl(it.toSeq.par)
}

class DataSetAPIScalaParallelImpl[T: ClassTag](private[sparklocal] val data: ParSeq[T]) extends DataSetAPIScalaBase[T] {
  override type InternalCollection = ParSeq[T]

  override private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]): DataSetAPIScalaBase[U] =
    DataSetAPIScalaParallelImpl.create(it)

  private def create[U: ClassTag](data: ParSeq[U]): DataSetAPIScalaBase[U] =
    new DataSetAPIScalaParallelImpl(data)

  override protected def union(data: InternalCollection, dsScala: DataSetAPIScalaBase[T]): DataSetAPIScalaBase[T] =
    create(data.union(dsScala.data.toSeq))

  override protected def intersect(data: InternalCollection, dsScala: DataSetAPIScalaBase[T]): DataSetAPIScalaBase[T] =
    create(data.intersect(dsScala.data.toSeq))

  override protected def diff(data: InternalCollection, dsScala: DataSetAPIScalaBase[T]): DataSetAPIScalaBase[T] =
    create(data.diff(dsScala.data.toSeq))

  override def distinct(): DataSetAPI[T] =
    create(data.distinct)

  override def groupByKey[K: ClassTag](func: (T) => K)(implicit enc: Encoder[K]): KeyValueGroupedDataSetAPI[K, T] =
    new KeyValueGroupedDataSetAPIScalaParallelImpl(data.groupBy(func))

  override def rdd(): RDDAPI[T] = RDDAPI(data)
}
