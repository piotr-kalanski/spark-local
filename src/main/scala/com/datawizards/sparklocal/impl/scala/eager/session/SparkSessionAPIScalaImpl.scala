package com.datawizards.sparklocal.impl.scala.eager.session

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.ReaderExecutor
import com.datawizards.sparklocal.impl.scala.eager.dataset.io.ReaderScalaImpl
import com.datawizards.sparklocal.rdd.RDDAPI
import com.datawizards.sparklocal.session.SparkSessionAPI
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

class SparkSessionAPIScalaImpl extends SparkSessionAPI {

  object implicits {
    implicit def enc[T]: Encoder[T] = null
  }

  override def createRDD[T: ClassTag](data: Seq[T]): RDDAPI[T] =
    RDDAPI(data)

  override def createDataset[T: ClassTag](data: Seq[T])(implicit enc: Encoder[T]): DataSetAPI[T] =
    DataSetAPI(data)

  override def read[T]: ReaderExecutor[T] =
    ReaderScalaImpl.read[T]

  override def textFile(path: String, minPartitions: Int=2): RDDAPI[String] =
    RDDAPI(scala.io.Source.fromFile(path).getLines().toIterable)

}
