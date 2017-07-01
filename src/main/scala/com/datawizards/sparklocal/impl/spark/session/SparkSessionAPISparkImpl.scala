package com.datawizards.sparklocal.impl.spark.session

import com.datawizards.sparklocal.accumulator.{AccumulatorV2API, CollectionAccumulatorAPI, DoubleAccumulatorAPI, LongAccumulatorAPI}
import com.datawizards.sparklocal.broadcast.BroadcastAPI
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.ReaderExecutor
import com.datawizards.sparklocal.impl.spark.accumulator.{AccumulatorV2APISparkImpl, CollectionAccumulatorAPISparkImpl, DoubleAccumulatorAPISparkImpl, LongAccumulatorAPISparkImpl}
import com.datawizards.sparklocal.impl.spark.broadcast.BroadcastAPISparkImpl
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

  override def broadcast[T: ClassTag](value: T): BroadcastAPI[T] =
    new BroadcastAPISparkImpl[T](spark.sparkContext.broadcast(value))

  override def register(acc: AccumulatorV2API[_, _], name: String): Unit = acc match {
    case a:AccumulatorV2APISparkImpl[_, _] => spark.sparkContext.register(a.acc, name)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot register ${acc.getClass.getName}")
  }

  override def register(acc: AccumulatorV2API[_, _]): Unit = acc match {
    case a:AccumulatorV2APISparkImpl[_, _] => spark.sparkContext.register(a.acc)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot register ${acc.getClass.getName}")
  }

  override def longAccumulator: LongAccumulatorAPI =
    new LongAccumulatorAPISparkImpl(spark.sparkContext.longAccumulator)

  override def longAccumulator(name: String): LongAccumulatorAPI =
    new LongAccumulatorAPISparkImpl(spark.sparkContext.longAccumulator(name))

  override def doubleAccumulator: DoubleAccumulatorAPI =
    new DoubleAccumulatorAPISparkImpl(spark.sparkContext.doubleAccumulator)

  override def doubleAccumulator(name: String): DoubleAccumulatorAPI =
    new DoubleAccumulatorAPISparkImpl(spark.sparkContext.doubleAccumulator(name))

  override def collectionAccumulator[T]: CollectionAccumulatorAPI[T] =
    new CollectionAccumulatorAPISparkImpl(spark.sparkContext.collectionAccumulator)

  override def collectionAccumulator[T](name: String): CollectionAccumulatorAPI[T] =
    new CollectionAccumulatorAPISparkImpl(spark.sparkContext.collectionAccumulator(name))

}
