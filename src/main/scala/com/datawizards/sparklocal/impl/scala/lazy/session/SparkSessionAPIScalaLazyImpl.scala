package com.datawizards.sparklocal.impl.scala.`lazy`.session

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.ReaderExecutor
import com.datawizards.sparklocal.impl.scala.`lazy`.dataset.io.ReaderScalaLazyImpl
import com.datawizards.sparklocal.impl.scala.session.SparkSessionAPIScalaBase
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

class SparkSessionAPIScalaLazyImpl extends SparkSessionAPIScalaBase {

  override def createRDD[T: ClassTag](data: Seq[T]): RDDAPI[T] =
    RDDAPI(data.view)

  override def createDataset[T: ClassTag](data: Seq[T])(implicit enc: Encoder[T]): DataSetAPI[T] =
    DataSetAPI(data.view)

  override def read[T]: ReaderExecutor[T] =
    ReaderScalaLazyImpl.read[T]

  override def textFile(path: String, minPartitions: Int=2): RDDAPI[String] =
    RDDAPI(scala.io.Source.fromFile(path).getLines().toSeq.view)

}
