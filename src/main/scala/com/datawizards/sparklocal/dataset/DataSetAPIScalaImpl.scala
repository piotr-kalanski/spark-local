package com.datawizards.sparklocal.dataset

import java.util

import com.datawizards.sparklocal.dataset.expressions.Expressions
import com.datawizards.sparklocal.dataset.io.{WriterExecutor, WriterScalaImpl}
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.{Column, Encoder}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.{BernoulliCellSampler, BernoulliSampler, PoissonSampler}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class DataSetAPIScalaImpl[T: ClassTag](iterable: Iterable[T]) extends DataSetAPI[T] {
  private[dataset] val data: Seq[T] = iterable.toSeq

  private def create[U: ClassTag](it: Iterable[U])(implicit enc: Encoder[U]=null) =
    new DataSetAPIScalaImpl(it)

  override private[dataset] def toDataset(implicit enc: Encoder[T]) = createDataset(data)

  override def map[That: ClassTag](map: T => That)(implicit enc: Encoder[That]): DataSetAPI[That] =
    create(data.map(map))

  override def collect(): Array[T] =
    data.toArray

  override def collectAsList(): java.util.List[T] =
    data

  override def filter(p: T => Boolean): DataSetAPI[T] =
    create(data.filter(p))

  override def count(): Long =
    data.size

  override def foreach(f: (T) => Unit): Unit =
    data.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit =
    f(data.iterator)

  override def head(): T =
    data.head

  override def head(n: Int): Array[T] =
    data.take(n).toArray

  override def reduce(func: (T, T) => T): T =
    data.reduce(func)

  override def cache(): DataSetAPI[T] =
    this

  override def checkpoint(eager: Boolean): DataSetAPI[T] =
    this

  override def persist(newLevel: StorageLevel): DataSetAPI[T] =
    this

  override def persist(): DataSetAPI[T] =
    this

  override def unpersist(): DataSetAPI[T] =
    this

  override def unpersist(blocking: Boolean): DataSetAPI[T] =
    this

  override def flatMap[U: ClassTag](func: (T) => TraversableOnce[U])(implicit enc: Encoder[U]=null): DataSetAPI[U] =
    create(data.flatMap(func))

  override def distinct(): DataSetAPI[T] =
    create(data.distinct)

  override def rdd(): RDDAPI[T] =
    RDDAPI(data)

  override def union(other: DataSetAPI[T])(implicit enc: Encoder[T]): DataSetAPI[T] = other match {
    case dsSpark:DataSetAPISparkImpl[T] => DataSetAPI(this.toDataset.union(dsSpark.data))
    case dsScala:DataSetAPIScalaImpl[T] => create(data.union(dsScala.data))
  }

  override def intersect(other: DataSetAPI[T])(implicit enc: Encoder[T]): DataSetAPI[T] = other match {
    case dsSpark:DataSetAPISparkImpl[T] => DataSetAPI(this.toDataset.intersect(dsSpark.data))
    case dsScala:DataSetAPIScalaImpl[T] => create(data.intersect(dsScala.data))
  }

  override def takeAsList(n: Int): util.List[T] =
    data.take(n)

  override def groupByKey[K: ClassTag](func: (T) => K)(implicit enc: Encoder[K]=null): KeyValueGroupedDataSetAPI[K, T] =
    new KeyValueGroupedDataSetAPIScalaImpl(data.groupBy(func))

  override def limit(n: Int): DataSetAPI[T] =
    create(data.take(n))

  override def repartition(numPartitions: Int): DataSetAPI[T] =
    this

  override def repartition(partitionExprs: Column*): DataSetAPI[T] =
    this

  override def repartition(numPartitions: Int, partitionExprs: Column*): DataSetAPI[T] =
    this

  override def coalesce(numPartitions: Int): DataSetAPI[T] =
    this

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataSetAPI[T] = {
    val sampler = if (withReplacement) new PoissonSampler[T](fraction) else new BernoulliSampler[T](fraction)
    sampler.setSeed(seed)
    create(sampler.sample(data.iterator).toIterable)
  }

  override def randomSplit(weights: Array[Double], seed: Long): Array[DataSetAPI[T]] = {
    require(weights.forall(_ >= 0),
      s"Weights must be nonnegative, but got ${weights.mkString("[", ",", "]")}")
    require(weights.sum > 0,
      s"Sum of weights must be positive, but got ${weights.mkString("[", ",", "]")}")

    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      val sampler = new BernoulliCellSampler[T](x(0), x(1))
      sampler.setSeed(seed)
      create(sampler.sample(data.iterator).toIterable)
    }.toArray
  }

  override def join[U: ClassTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression)
                                (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)] = other match {
    case dsScala:DataSetAPIScalaImpl[U] => create(
      for {
        left <- data
        right <- dsScala.data
        if condition.eval(left, right)
      } yield (left, right)
    )
    case dsSpark:DataSetAPISparkImpl[U] => DataSetAPI(this.toDataset.joinWith(dsSpark.data, condition.toSparkColumn, "inner"))
  }

  override def leftOuterJoin[U: ClassTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression)
                                         (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)] = other match {
    case dsScala:DataSetAPIScalaImpl[U] =>
      val b = new ListBuffer[(T,U)]

      val empty: U = null.asInstanceOf[U]

      for (left <- data) {
        var rightExists = false
        for (right <- dsScala.data) {
          if (condition.eval(left, right)) {
            b += ((left, right))
            rightExists = true
          }
        }
        if(!rightExists) {
          b += ((left, empty))
        }
      }

      create(b)
    case dsSpark:DataSetAPISparkImpl[U] => DataSetAPI(this.toDataset.joinWith(dsSpark.data, condition.toSparkColumn, "left_outer"))
  }

  override def rightOuterJoin[U: ClassTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression)
                                          (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)] = other match {
    case dsScala:DataSetAPIScalaImpl[U] =>
      val b = new ListBuffer[(T,U)]

      val empty: T = null.asInstanceOf[T]

      for (right <- dsScala.data) {
        var leftExists = false
        for (left <- data) {
          if (condition.eval(left, right)) {
            b += ((left, right))
            leftExists = true
          }
        }
        if(!leftExists) {
          b += (empty -> right)
        }
      }

      create(b)
    case dsSpark:DataSetAPISparkImpl[U] => DataSetAPI(this.toDataset.joinWith(dsSpark.data, condition.toSparkColumn, "right_outer"))
  }

  override def fullOuterJoin[U: ClassTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression)
                                         (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)] = other match {
    case dsScala:DataSetAPIScalaImpl[U] =>
      val b = new ListBuffer[(T,U)]

      val emptyT: T = null.asInstanceOf[T]
      val emptyU: U = null.asInstanceOf[U]

      for (left <- data) {
        var rightExists = false
        for (right <- dsScala.data) {
          if (condition.eval(left, right)) {
            b += ((left, right))
            rightExists = true
          }
        }
        if(!rightExists) {
          b += ((left, emptyU))
        }
      }
      for (right <- dsScala.data) {
        var leftExists = false
        for (left <- data) {
          if (condition.eval(left, right)) {
            leftExists = true
          }
        }
        if(!leftExists) {
          b += ((emptyT, right))
        }
      }

      create(b)
    case dsSpark:DataSetAPISparkImpl[U] => DataSetAPI(this.toDataset.joinWith(dsSpark.data, condition.toSparkColumn, "outer"))
  }

  override def write: WriterExecutor[T] = new WriterScalaImpl[T].write(this)

}
