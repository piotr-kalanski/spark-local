package com.datawizards.sparklocal.impl.scala.parallel.rdd

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.impl.scala.rdd.RDDAPIScalaBase
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.Encoder

import scala.collection.GenIterable
import scala.collection.parallel.ParSeq
import scala.reflect.ClassTag

class RDDAPIScalaParallelImpl[T: ClassTag](private[sparklocal] val data: ParSeq[T]) extends RDDAPIScalaBase[T] {
  override type InternalCollection = ParSeq[T]

  override private[sparklocal] def create[U: ClassTag](data: GenIterable[U]): RDDAPIScalaBase[U] =
    create(data.toSeq.par)

  protected def create[U: ClassTag](data: ParSeq[U]): RDDAPIScalaBase[U] =
    new RDDAPIScalaParallelImpl(data)

  override protected def union(data: InternalCollection, rddScala: RDDAPIScalaBase[T]): RDDAPI[T] =
    create(data.union(rddScala.data.toSeq.par))

  override protected def intersect(data: InternalCollection, rddScala: RDDAPIScalaBase[T]): RDDAPI[T] =
    create(data.intersect(rddScala.data.toSeq.par))

  override protected def diff(data: InternalCollection, rddScala: RDDAPIScalaBase[T]): RDDAPI[T] =
    create(data.diff(rddScala.data.toSeq.par))

  override def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] =
    data.toVector.sorted.take(num).toArray

  override def cache(): RDDAPI[T] = this

  override def sortBy[K](f: (T) => K, ascending: Boolean, numPartitions: Int)(implicit ord: Ordering[K], ctag: ClassTag[K]): RDDAPI[T] =
    create(data.toVector.sortBy(f)(if(ascending) ord else ord.reverse))

  override def distinct(): RDDAPI[T] =
    create(data.distinct)

  override def top(num: Int)(implicit ord: Ordering[T]): Array[T] =
    data.toVector.sorted(ord.reverse).take(num).toArray

  override def toDataSet(implicit enc: Encoder[T]): DataSetAPI[T] =
    DataSetAPI(data)
}
