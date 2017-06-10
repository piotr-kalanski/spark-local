package com.datawizards.sparklocal.impl.scala.parallellazy.dataset.io

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.impl.scala.dataset.io.ReaderScalaBase
import com.datawizards.sparklocal.impl.scala.parallellazy.ParallelLazySeq

import scala.reflect.ClassTag

object ReaderScalaParallelLazyImpl extends ReaderScalaBase {
  override protected def createDataSet[T: ClassTag](iterable: Iterable[T]): DataSetAPI[T] =
    DataSetAPI(new ParallelLazySeq(iterable.toSeq.par))
}
