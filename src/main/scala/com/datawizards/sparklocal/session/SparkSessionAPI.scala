package com.datawizards.sparklocal.session

import java.lang

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.dataset.io.{ReaderExecutor, ReaderScalaImpl, ReaderSparkImpl}
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.{Encoder, SQLContext, SQLImplicits, SparkSession}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

object SparkSessionAPI {

  /**
    * Creates a [[Builder]] for constructing a [[SparkSessionAPI]].
    *
    * @param engine Spark or Scala implementation
    */
  def builder(engine: ExecutionEngine.ExecutionEngine): Builder = engine match {
    case ExecutionEngine.ScalaEager => new BuilderScalaImpl
    case ExecutionEngine.Spark => new BuilderSparkImpl(SparkSession.builder())
  }

}

trait SparkSessionImplicits {

  implicit def newProductEncoder[T <: Product](implicit tt: TypeTag[T]=null): Encoder[T]

  // Primitives

  implicit def newIntEncoder: Encoder[Int]

  implicit def newLongEncoder: Encoder[Long]

  implicit def newDoubleEncoder: Encoder[Double]

  implicit def newFloatEncoder: Encoder[Float]

  implicit def newByteEncoder: Encoder[Byte]

  implicit def newShortEncoder: Encoder[Short]

  implicit def newBooleanEncoder: Encoder[Boolean]

  implicit def newStringEncoder: Encoder[String]

  // Boxed primitives

  implicit def newBoxedIntEncoder: Encoder[java.lang.Integer]

  implicit def newBoxedLongEncoder: Encoder[java.lang.Long]

  implicit def newBoxedDoubleEncoder: Encoder[java.lang.Double]

  implicit def newBoxedFloatEncoder: Encoder[java.lang.Float]

  implicit def newBoxedByteEncoder: Encoder[java.lang.Byte]

  implicit def newBoxedShortEncoder: Encoder[java.lang.Short]

  implicit def newBoxedBooleanEncoder: Encoder[java.lang.Boolean]

  // Seqs

  implicit def newIntSeqEncoder: Encoder[Seq[Int]]

  implicit def newLongSeqEncoder: Encoder[Seq[Long]]

  implicit def newDoubleSeqEncoder: Encoder[Seq[Double]]

  implicit def newFloatSeqEncoder: Encoder[Seq[Float]]

  implicit def newByteSeqEncoder: Encoder[Seq[Byte]]

  implicit def newShortSeqEncoder: Encoder[Seq[Short]]

  implicit def newBooleanSeqEncoder: Encoder[Seq[Boolean]]

  implicit def newStringSeqEncoder: Encoder[Seq[String]]

  implicit def newProductSeqEncoder[A <: Product](implicit tt: TypeTag[A]=null): Encoder[Seq[A]]

  // Arrays

  implicit def newIntArrayEncoder: Encoder[Array[Int]]

  implicit def newLongArrayEncoder: Encoder[Array[Long]]

  implicit def newDoubleArrayEncoder: Encoder[Array[Double]]

  implicit def newFloatArrayEncoder: Encoder[Array[Float]]

  implicit def newByteArrayEncoder: Encoder[Array[Byte]]

  implicit def newShortArrayEncoder: Encoder[Array[Short]]

  implicit def newBooleanArrayEncoder: Encoder[Array[Boolean]]

  implicit def newStringArrayEncoder: Encoder[Array[String]]

  implicit def newProductArrayEncoder[A <: Product](implicit tt: TypeTag[A]=null): Encoder[Array[A]]
}

trait SparkSessionAPI {

  /**
    * Implicit methods available in Scala for converting
    * common Scala objects into Datasets
    */
  val implicits: SparkSessionImplicits

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

object SparkSessionAPIScalaImpl extends SparkSessionAPI {

  //  Scala implementation doesn't need implicit conversions
    override val implicits: SparkSessionImplicits = new SparkSessionImplicits {

      override implicit def newByteArrayEncoder: Encoder[Array[Byte]] = null

      override implicit def newDoubleSeqEncoder: Encoder[Seq[Double]] = null

      override implicit def newLongArrayEncoder: Encoder[Array[Long]] = null

      override implicit def newShortArrayEncoder: Encoder[Array[Short]] = null

      override implicit def newIntSeqEncoder: Encoder[Seq[Int]] = null

      override implicit def newStringSeqEncoder: Encoder[Seq[String]] = null

      override implicit def newStringArrayEncoder: Encoder[Array[String]] = null

      override implicit def newBoxedFloatEncoder: Encoder[lang.Float] = null

      override implicit def newFloatSeqEncoder: Encoder[Seq[Float]] = null

      override implicit def newBooleanEncoder: Encoder[Boolean] = null

      override implicit def newIntEncoder: Encoder[Int] = null

      override implicit def newFloatEncoder: Encoder[Float] = null

      override implicit def newBoxedIntEncoder: Encoder[Integer] = null

      override implicit def newByteSeqEncoder: Encoder[Seq[Byte]] = null

      override implicit def newBoxedLongEncoder: Encoder[lang.Long] = null

      override implicit def newBoxedByteEncoder: Encoder[lang.Byte] = null

      override implicit def newBoxedShortEncoder: Encoder[lang.Short] = null

      override implicit def newDoubleEncoder: Encoder[Double] = null

      override implicit def newProductEncoder[T <: Product](implicit tt: universe.TypeTag[T]=null): Encoder[T] = null

      override implicit def newProductArrayEncoder[A <: Product](implicit tt: universe.TypeTag[A]=null): Encoder[Array[A]] = null

      override implicit def newBooleanSeqEncoder: Encoder[Seq[Boolean]] = null

      override implicit def newDoubleArrayEncoder: Encoder[Array[Double]] = null

      override implicit def newShortSeqEncoder: Encoder[Seq[Short]] = null

      override implicit def newBoxedBooleanEncoder: Encoder[lang.Boolean] = null

      override implicit def newByteEncoder: Encoder[Byte] = null

      override implicit def newBoxedDoubleEncoder: Encoder[lang.Double] = null

      override implicit def newShortEncoder: Encoder[Short] = null

      override implicit def newBooleanArrayEncoder: Encoder[Array[Boolean]] = null

      override implicit def newFloatArrayEncoder: Encoder[Array[Float]] = null

      override implicit def newStringEncoder: Encoder[String] = null

      override implicit def newLongEncoder: Encoder[Long] = null

      override implicit def newProductSeqEncoder[A <: Product](implicit tt: universe.TypeTag[A]): Encoder[Seq[A]] = null

      override implicit def newIntArrayEncoder: Encoder[Array[Int]] = null

      override implicit def newLongSeqEncoder: Encoder[Seq[Long]] = null
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

class SparkSessionAPISparkImpl(private [session] val spark: SparkSession) extends SparkSessionAPI {

  object implicitImpl extends SQLImplicits with SparkSessionImplicits {
    override protected def _sqlContext: SQLContext = spark.sqlContext
  }

  override val implicits: SparkSessionImplicits = implicitImpl

  override def createRDD[T: ClassTag](data: Seq[T]): RDDAPI[T] =
    RDDAPI(spark.sparkContext.parallelize(data))

  override def createDataset[T: ClassTag](data: Seq[T])(implicit enc: Encoder[T]): DataSetAPI[T] =
    DataSetAPI(spark.createDataset(data))

  override def read[T]: ReaderExecutor[T] =
    ReaderSparkImpl.read[T]

  override def textFile(path: String, minPartitions: Int=2): RDDAPI[String] =
    RDDAPI(spark.sparkContext.textFile(path, minPartitions))
}
