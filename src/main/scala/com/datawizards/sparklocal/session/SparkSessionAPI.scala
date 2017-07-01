package com.datawizards.sparklocal.session

import com.datawizards.sparklocal.accumulator.{AccumulatorV2API, CollectionAccumulatorAPI, DoubleAccumulatorAPI, LongAccumulatorAPI}
import com.datawizards.sparklocal.broadcast.BroadcastAPI
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
    * Returns a ReaderExecutor that can be used to read non-streaming data in as a DataSet
    */
  def read[T]: ReaderExecutor[T]

  /**
    * Read a text file from HDFS, a local file system (available on all nodes), or any
    * Hadoop-supported file system URI, and return it as an RDD of Strings.
    */
  def textFile(path: String, minPartitions: Int = 2): RDDAPI[String]

  /**
    * Register the given accumulator with given name.
    */
  def register(acc: AccumulatorV2API[_, _], name: String): Unit

  /**
    * Register the given accumulator.
    */
  def register(acc: AccumulatorV2API[_, _]): Unit

  /**
    * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
    */
  def longAccumulator: LongAccumulatorAPI

  /**
    * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
    */
  def longAccumulator(name: String): LongAccumulatorAPI

  /**
    * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
    */
  def doubleAccumulator: DoubleAccumulatorAPI

  /**
    * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
    */
  def doubleAccumulator(name: String): DoubleAccumulatorAPI

  /**
    * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
    * inputs by adding them into the list.
    */
  def collectionAccumulator[T]: CollectionAccumulatorAPI[T]

  /**
    * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
    * inputs by adding them into the list.
    */
  def collectionAccumulator[T](name: String): CollectionAccumulatorAPI[T]

  /**
    * Broadcast a read-only variable to the cluster, returning a
    * broadcast object for reading it in distributed functions.
    * The variable will be sent to each cluster only once.
    */
  def broadcast[T: ClassTag](value: T): BroadcastAPI[T]
}
