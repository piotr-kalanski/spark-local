package com.datawizards.sparklocal.impl.scala.parallel.session

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.ReaderExecutor
import com.datawizards.sparklocal.impl.scala.parallel.dataset.io.ReaderScalaParallelImpl
import com.datawizards.sparklocal.impl.scala.session.SparkSessionAPIScalaBase
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

class SparkSessionAPIScalaParallelImpl extends SparkSessionAPIScalaBase {

  override def createRDD[T: ClassTag](data: Seq[T]): RDDAPI[T] =
    RDDAPI(data.par)

  override def createDataset[T: ClassTag](data: Seq[T])(implicit enc: Encoder[T]): DataSetAPI[T] =
    DataSetAPI(data.par)

  override def read[T]: ReaderExecutor[T] =
    ReaderScalaParallelImpl.read[T]

  override def textFile(path: String, minPartitions: Int=2): RDDAPI[String] =
    RDDAPI(scala.io.Source.fromFile(path).getLines().toSeq.par)

}
