package com.datawizards.sparklocal.impl.scala.rdd

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.impl.spark.rdd.RDDAPISparkImpl
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.rdd.{PartitionCoalescer, RDD}
import org.apache.spark.sql.Encoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.{BernoulliCellSampler, BernoulliSampler, PoissonSampler}
import org.apache.spark.{Partition, Partitioner}

import scala.collection.GenIterable
import scala.reflect.ClassTag

abstract class RDDAPIScalaBase[T: ClassTag] extends RDDAPI[T] {
  type InternalCollection <: GenIterable[T]

  private[sparklocal] val data: InternalCollection

  override private[sparklocal] def toRDD = parallelize(data)

  private[sparklocal] def create[U: ClassTag](data: GenIterable[U]): RDDAPIScalaBase[U]

  override def collect(): Array[T] = data.toArray

  override def map[That: ClassTag](map: (T) => That): RDDAPI[That] = create(data.map(map))

  override def filter(p: (T) => Boolean): RDDAPI[T] = create(data.filter(p))

  override def flatMap[U: ClassTag](func: (T) => TraversableOnce[U]): RDDAPI[U] =
    create(data.flatMap(func))

  override def reduce(func: (T, T) => T): T = data.reduce(func)

  override def fold(zeroValue: T)(op: (T, T) => T): T = data.fold(zeroValue)(op)

  override def head(): T = data.head

  override def head(n: Int): Array[T] = data.take(n).toArray

  override def isEmpty: Boolean = data.isEmpty

  override def zip[U: ClassTag](other: RDDAPI[U]): RDDAPI[(T, U)] = other match {
    case rddScala:RDDAPIScalaBase[U] => create(data zip rddScala.data)
    case rddSpark:RDDAPISparkImpl[U] => RDDAPI(parallelize(data) zip rddSpark.data)
  }

  override def foreach(f: (T) => Unit): Unit = data.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = f(data.iterator)

  override def checkpoint(): RDDAPI[T] = this

  override def persist(newLevel: StorageLevel): RDDAPI[T] = cache()

  override def persist(): RDDAPI[T] = cache()

  override def unpersist(blocking: Boolean): RDDAPI[T] = this

  override def union(other: RDDAPI[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaBase[T] => union(data, rddScala)
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(parallelize(data) union rddSpark.data)
  }

  override def zipWithIndex(): RDDAPI[(T, Long)] = create(data zip (0L until data.size))

  override def min()(implicit ord: Ordering[T]): T = data.min

  override def max()(implicit ord: Ordering[T]): T = data.max

  override def partitions: Array[Partition] = Array.empty

  override def intersection(other: RDDAPI[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaBase[T] => intersect(data, rddScala)
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(parallelize(data) intersection rddSpark.data)
  }

  override def intersection(other: RDDAPI[T], numPartitions: Int): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaBase[T] => intersect(data, rddScala)
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(parallelize(data).intersection(rddSpark.data, numPartitions))
  }

  override def intersection(other: RDDAPI[T], partitioner: Partitioner)(implicit ord: Ordering[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaBase[T] => intersect(data, rddScala)
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(parallelize(data).intersection(rddSpark.data, partitioner)(ord))
  }

  override def count(): Long = data.size

  override def distinct(numPartitions: Int)(implicit ord: Ordering[T]): RDDAPI[T] = distinct()

  override def subtract(other: RDDAPI[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaBase[T] => diff(data, rddScala)
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(parallelize(data).subtract(rddSpark.data))
  }

  override def subtract(other: RDDAPI[T], numPartitions: Int): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaBase[T] => diff(data, rddScala)
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(parallelize(data).subtract(rddSpark.data, numPartitions))
  }

  override def subtract(other: RDDAPI[T], partitioner: Partitioner)(implicit ord: Ordering[T]): RDDAPI[T] = other match {
    case rddScala:RDDAPIScalaBase[T] => diff(data, rddScala)
    case rddSpark:RDDAPISparkImpl[T] => RDDAPI(parallelize(data).subtract(rddSpark.data, partitioner)(ord))
  }

  override def cartesian[U: ClassTag](other: RDDAPI[U]): RDDAPI[(T, U)] = other match {
    case rddScala:RDDAPIScalaBase[U] => create(
      for{
        left <- data
        right <- rddScala.data
      } yield (left, right)
    )
    case rddSpark:RDDAPISparkImpl[U] => RDDAPI(parallelize(data).cartesian(rddSpark.data))
  }

  override def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U =
    data.aggregate(zeroValue)(seqOp, combOp)

  override def groupBy[K](f: (T) => K)(implicit kt: ClassTag[K]): RDDAPI[(K, GenIterable[T])] =
    this.map(t => (f(t), t)).groupByKey()

  override def groupBy[K](f: (T) => K, numPartitions: Int)(implicit kt: ClassTag[K]): RDDAPI[(K, GenIterable[T])] =
    groupBy(f)

  override def groupBy[K](f: (T) => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K]): RDDAPI[(K, GenIterable[T])] =
    groupBy(f)

  override def coalesce(numPartitions: Int, shuffle: Boolean, partitionCoalescer: Option[PartitionCoalescer])(implicit ord: Ordering[T]): RDDAPI[T] = this

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): RDDAPI[T] = {
    val sampler = if (withReplacement) new PoissonSampler[T](fraction) else new BernoulliSampler[T](fraction)
    sampler.setSeed(seed)
    RDDAPI(sampler.sample(data.iterator).toIterable)
  }

  override def takeSample(withReplacement: Boolean, num: Int, seed: Long): Array[T] =
    sample(withReplacement, 2.0 * num/data.size, seed).take(num)

  override def randomSplit(weights: Array[Double], seed: Long): Array[RDDAPI[T]] = {
    require(weights.forall(_ >= 0),
      s"Weights must be nonnegative, but got ${weights.mkString("[", ",", "]")}")
    require(weights.sum > 0,
      s"Sum of weights must be positive, but got ${weights.mkString("[", ",", "]")}")

    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      val sampler = new BernoulliCellSampler[T](x(0), x(1))
      sampler.setSeed(seed)
      RDDAPI(sampler.sample(data.iterator).toIterable)
    }.toArray
  }

  override def toDataSet(implicit enc: Encoder[T]): DataSetAPI[T]

  protected def union(data: InternalCollection, rddScala: RDDAPIScalaBase[T]): RDDAPI[T]

  protected def intersect(data: InternalCollection, rddScala: RDDAPIScalaBase[T]): RDDAPI[T]

  protected def diff(data: InternalCollection, rddScala: RDDAPIScalaBase[T]): RDDAPI[T]
}
