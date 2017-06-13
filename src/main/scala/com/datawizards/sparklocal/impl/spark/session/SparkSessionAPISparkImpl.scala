package com.datawizards.sparklocal.impl.spark.session

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.ReaderExecutor
import com.datawizards.sparklocal.impl.spark.dataset.io.ReaderSparkImpl
import com.datawizards.sparklocal.rdd.RDDAPI
import com.datawizards.sparklocal.session.SparkSessionAPI
import org.apache.spark.sql.{Encoder, SQLContext, SQLImplicits, SparkSession}

import scala.reflect.ClassTag

class SparkSessionAPISparkImpl(private [sparklocal] val spark: SparkSession) extends SparkSessionAPI {

  object implicitImpl extends SQLImplicits {
    override protected def _sqlContext: SQLContext = spark.sqlContext
  }

  val implicits: SQLImplicits = implicitImpl

  override def createRDD[T: ClassTag](data: Seq[T]): RDDAPI[T] =
    RDDAPI(spark.sparkContext.parallelize(data))

  override def createDataset[T: ClassTag](data: Seq[T])(implicit enc: Encoder[T]): DataSetAPI[T] =
    DataSetAPI(spark.createDataset(data))

  override def read[T]: ReaderExecutor[T] =
    ReaderSparkImpl.read[T]

  override def textFile(path: String, minPartitions: Int=2): RDDAPI[String] =
    RDDAPI(spark.sparkContext.textFile(path, minPartitions))
}
