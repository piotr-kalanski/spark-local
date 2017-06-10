package com.datawizards.sparklocal.impl.scala.parallel.dataset.io

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.impl.scala.dataset.io.ReaderScalaBase

import scala.reflect.ClassTag

object ReaderScalaParallelImpl extends ReaderScalaBase {
  override protected def createDataSet[T: ClassTag](iterable: Iterable[T]): DataSetAPI[T] =
    DataSetAPI(iterable.toSeq.par)
}
