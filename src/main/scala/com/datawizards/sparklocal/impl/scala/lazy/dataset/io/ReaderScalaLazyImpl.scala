package com.datawizards.sparklocal.impl.scala.`lazy`.dataset.io

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.impl.scala.dataset.io.ReaderScalaBase

import scala.reflect.ClassTag

object ReaderScalaLazyImpl extends ReaderScalaBase {
  override protected def createDataSet[T: ClassTag](iterable: Iterable[T]): DataSetAPI[T] =
    DataSetAPI(iterable.toSeq.view)
}
