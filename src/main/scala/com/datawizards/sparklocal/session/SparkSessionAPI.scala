package com.datawizards.sparklocal.session

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.ReaderExecutor
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

object SparkSessionAPI {

  /**
    * Creates a [[Builder]] for constructing a [[SparkSessionAPI]].
    *
    * @param engine Spark or Scala implementation
    */
  def builder[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): Builder[Session] = engine.builder()

}

trait SparkSessionAPI {

  /**
    * Create new RDD based on Scala collection
    */
  def createRDD[T: ClassTag](data: Seq[T]): RDDAPI[T]

  /**
    * Create new DataSet based on Scala collection
    */
  def createDataset[T: ClassTag](data: Seq[T])(implicit enc: Encoder[T]): DataSetAPI[T]

  /**
    * Create new DataSet based on RDD
    */
  def createDataset[T: ClassTag](data: RDDAPI[T])(implicit enc: Encoder[T]): DataSetAPI[T] =
    data.toDataSet

  /**
    * Returns a [[ReaderExecutor]] that can be used to read non-streaming data in as a DataSet
    */
  def read[T]: ReaderExecutor[T]

  /**
    * Read a text file from HDFS, a local file system (available on all nodes), or any
    * Hadoop-supported file system URI, and return it as an RDD of Strings.
    */
  def textFile(path: String, minPartitions: Int = 2): RDDAPI[String]

  //TODO - accumulator
  /*

  def register(acc: AccumulatorV2[_, _]): Unit = {
    acc.register(this)
  }

  def register(acc: AccumulatorV2[_, _], name: String): Unit = {
    acc.register(this, name = Some(name))
  }

  def longAccumulator: LongAccumulator = {
    val acc = new LongAccumulator
    register(acc)
    acc
  }


  def longAccumulator(name: String): LongAccumulator = {
    val acc = new LongAccumulator
    register(acc, name)
    acc
  }


  def doubleAccumulator: DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc)
    acc
  }

  def doubleAccumulator(name: String): DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc, name)
    acc
  }


  def collectionAccumulator[T]: CollectionAccumulator[T] = {
    val acc = new CollectionAccumulator[T]
    register(acc)
    acc
  }

  */

  //TODO - broadcast
  //def broadcast[T: ClassTag](value: T): Broadcast[T]
}
