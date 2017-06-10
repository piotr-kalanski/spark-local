package com.datawizards.sparklocal.impl.scala.parallellazy.session

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.ReaderExecutor
import com.datawizards.sparklocal.impl.scala.parallellazy.ParallelLazySeq
import com.datawizards.sparklocal.impl.scala.parallellazy.dataset.io.ReaderScalaParallelLazyImpl
import com.datawizards.sparklocal.impl.scala.session.SparkSessionAPIScalaBase
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

class SparkSessionAPIScalaParallelLazyImpl extends SparkSessionAPIScalaBase {

  override def createRDD[T: ClassTag](data: Seq[T]): RDDAPI[T] =
    RDDAPI(new ParallelLazySeq(data.par))

  override def createDataset[T: ClassTag](data: Seq[T])(implicit enc: Encoder[T]): DataSetAPI[T] =
    DataSetAPI(new ParallelLazySeq(data.par))

  override def read[T]: ReaderExecutor[T] =
    ReaderScalaParallelLazyImpl.read[T]

  override def textFile(path: String, minPartitions: Int=2): RDDAPI[String] =
    RDDAPI(new ParallelLazySeq(scala.io.Source.fromFile(path).getLines().toSeq.par))

}
