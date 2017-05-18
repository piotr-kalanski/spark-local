package com.datawizards.sparklocal.dataset

import java.util

import com.datawizards.sparklocal.dataset.expressions.Expressions
import com.datawizards.sparklocal.dataset.io.{WriterExecutor, WriterSparkImpl}
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.{Column, Dataset, Encoder}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class DataSetAPISparkImpl[T: ClassTag](val data: Dataset[T]) extends DataSetAPI[T] {

  private def create[U: ClassTag](ds: Dataset[U]) = new DataSetAPISparkImpl(ds)

  override private[dataset] def toDataset(implicit enc: Encoder[T]) = data

  override def map[That: ClassTag](map: T => That)(implicit enc: Encoder[That]): DataSetAPI[That] =
    create(data.map(map))

  override def collect(): Array[T] =
    data.collect()

  override def collectAsList(): java.util.List[T] =
    data.collectAsList()

  override def filter(p: T => Boolean): DataSetAPI[T] =
    create(data.filter(p))

  override def count(): Long =
    data.count()

  override def foreach(f: (T) => Unit): Unit =
    data.foreach(f)

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit =
    data.foreachPartition(f)

  override def head(): T =
    data.head()

  override def head(n: Int): Array[T] =
    data.head(n)

  override def reduce(func: (T, T) => T): T =
    data.reduce(func)

  override def cache(): DataSetAPI[T] =
    create(data.cache())

  override def checkpoint(eager: Boolean): DataSetAPI[T] =
    create(data.checkpoint(eager))

  override def persist(newLevel: StorageLevel): DataSetAPI[T] =
    create(data.persist(newLevel))

  override def persist(): DataSetAPI[T] =
    create(data.persist())

  override def unpersist(): DataSetAPI[T] =
    create(data.unpersist())

  override def unpersist(blocking: Boolean): DataSetAPI[T] =
    create(data.unpersist(blocking))

  override def flatMap[U: ClassTag](func: (T) => TraversableOnce[U])(implicit enc: Encoder[U]=null): DataSetAPI[U] =
    create(data.flatMap(func))

  override def distinct(): DataSetAPI[T] =
    create(data.distinct)

  override def rdd(): RDDAPI[T] =
    RDDAPI(data.rdd)

  override def union(other: DataSetAPI[T])(implicit enc: Encoder[T]): DataSetAPI[T] =
    create(data.union(other.toDataset))

  override def intersect(other: DataSetAPI[T])(implicit enc: Encoder[T]): DataSetAPI[T] =
    create(data.intersect(other.toDataset))

  override def takeAsList(n: Int): util.List[T] =
    data.takeAsList(n)

  override def groupByKey[K: ClassTag](func: (T) => K)(implicit enc: Encoder[K]=null): KeyValueGroupedDataSetAPI[K, T] =
    new KeyValueGroupedDataSetAPISparkImpl(data.groupByKey(func))

  override def limit(n: Int): DataSetAPI[T] =
    create(data.limit(n))

  override def repartition(numPartitions: Int): DataSetAPI[T] =
    create(data.repartition(numPartitions))

  override def repartition(partitionExprs: Column*): DataSetAPI[T] =
    create(data.repartition(partitionExprs:_*))

  override def repartition(numPartitions: Int, partitionExprs: Column*): DataSetAPI[T] =
    create(data.repartition(numPartitions, partitionExprs:_*))

  override def coalesce(numPartitions: Int): DataSetAPI[T] =
    create(data.coalesce(numPartitions))

  override def sample(withReplacement: Boolean, fraction: Double, seed: Long): DataSetAPI[T] =
    create(data.sample(withReplacement, fraction, seed))

  override def randomSplit(weights: Array[Double], seed: Long): Array[DataSetAPI[T]] =
    data.randomSplit(weights, seed).map(ds => create(ds))

  override def join[U: ClassTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression)
                                (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)] =
    create(data.joinWith(other.toDataset, condition.toSparkColumn, "inner"))

  override def leftOuterJoin[U: ClassTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression)
                                         (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)] =
    create(data.joinWith(other.toDataset, condition.toSparkColumn, "left_outer"))

  override def rightOuterJoin[U: ClassTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression)
                                          (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)] =
    create(data.joinWith(other.toDataset, condition.toSparkColumn, "right_outer"))

  override def fullOuterJoin[U: ClassTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression)
                                         (implicit encT: Encoder[T], encU: Encoder[U], encTU: Encoder[(T,U)]): DataSetAPI[(T, U)] =
    create(data.joinWith(other.toDataset, condition.toSparkColumn, "outer"))

  override def write: WriterExecutor[T] = new WriterSparkImpl[T].write(this)

}
