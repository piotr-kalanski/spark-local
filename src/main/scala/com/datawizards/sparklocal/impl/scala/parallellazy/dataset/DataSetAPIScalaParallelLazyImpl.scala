package com.datawizards.sparklocal.impl.scala.parallellazy.dataset

import com.datawizards.sparklocal.dataset.{DataSetAPI, KeyValueGroupedDataSetAPI}
import com.datawizards.sparklocal.impl.scala.dataset.DataSetAPIScalaBase
import com.datawizards.sparklocal.impl.scala.parallellazy.ParallelLazySeq
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.Encoder

import scala.collection.GenIterable
import scala.reflect.ClassTag

object DataSetAPIScalaParallelLazyImpl {
  private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]): DataSetAPIScalaBase[U] =
    new DataSetAPIScalaParallelLazyImpl(new ParallelLazySeq(it.toSeq.par))
}

class DataSetAPIScalaParallelLazyImpl[T: ClassTag](private[sparklocal] val data: ParallelLazySeq[T]) extends DataSetAPIScalaBase[T] {
  override type InternalCollection = ParallelLazySeq[T]

  override private[sparklocal] def create[U: ClassTag](it: GenIterable[U])(implicit enc: Encoder[U]): DataSetAPIScalaBase[U] =
    DataSetAPIScalaParallelLazyImpl.create(it)

  private def create[U: ClassTag](data: ParallelLazySeq[U]): DataSetAPIScalaBase[U] =
    new DataSetAPIScalaParallelLazyImpl(data)

  override protected def union(data: InternalCollection, dsScala: DataSetAPIScalaBase[T]): DataSetAPIScalaBase[T] =
    create(data.union(dsScala.data.toSeq))

  override protected def intersect(data: InternalCollection, dsScala: DataSetAPIScalaBase[T]): DataSetAPIScalaBase[T] =
    create(data.intersect(dsScala.data.toSeq))

  override protected def diff(data: InternalCollection, dsScala: DataSetAPIScalaBase[T]): DataSetAPIScalaBase[T] =
    create(data.diff(dsScala.data.toSeq))

  override def distinct(): DataSetAPI[T] =
    create(data.distinct)

  override def groupByKey[K: ClassTag](func: (T) => K)(implicit enc: Encoder[K]): KeyValueGroupedDataSetAPI[K, T] =
    new KeyValueGroupedDataSetAPIScalaParallelLazyImpl(data.groupBy(func))

  override def rdd(): RDDAPI[T] = RDDAPI(data)
}
