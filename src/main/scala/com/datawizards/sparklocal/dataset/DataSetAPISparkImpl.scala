package com.datawizards.sparklocal.dataset

import java.util

import com.datawizards.sparklocal.dataset.expressions.Expressions
import com.datawizards.sparklocal.dataset.io.{Writer, WriterExecutor, WriterSparkImpl}
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class DataSetAPISparkImpl[T: ClassTag: TypeTag](val data: Dataset[T]) extends DataSetAPI[T] {

  private def create[U: ClassTag: TypeTag](ds: Dataset[U]) = new DataSetAPISparkImpl(ds)

  override private[dataset] def toDataset = data

  override def map[That: ClassTag: TypeTag](map: T => That): DataSetAPI[That] =
    create(data.map(map)(ExpressionEncoder[That]()))

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

  override def flatMap[U: ClassTag: TypeTag](func: (T) => TraversableOnce[U]): DataSetAPI[U] =
    create(data.flatMap(func)(ExpressionEncoder[U]()))

  override def distinct(): DataSetAPI[T] =
    create(data.distinct)

  override def rdd(): RDDAPI[T] =
    RDDAPI(data.rdd)

  override def union(other: DataSetAPI[T]): DataSetAPI[T] =
    create(data.union(other.toDataset))

  override def intersect(other: DataSetAPI[T]): DataSetAPI[T] =
    create(data.intersect(other.toDataset))

  override def takeAsList(n: Int): util.List[T] =
    data.takeAsList(n)

  override def groupByKey[K: ClassTag: TypeTag](func: (T) => K): KeyValueGroupedDataSetAPI[K, T] =
    new KeyValueGroupedDataSetAPISparkImpl(data.groupByKey(func)(ExpressionEncoder[K]()))

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

  override def join[U: ClassTag : TypeTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression): DataSetAPI[(T, U)] =
    create(data.joinWith(other.toDataset, condition.toSparkColumn, "inner"))

  override def leftOuterJoin[U: ClassTag : TypeTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression): DataSetAPI[(T, U)] =
    create(data.joinWith(other.toDataset, condition.toSparkColumn, "left_outer"))

  override def rightOuterJoin[U: ClassTag : TypeTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression): DataSetAPI[(T, U)] =
    create(data.joinWith(other.toDataset, condition.toSparkColumn, "right_outer"))

  override def fullOuterJoin[U: ClassTag : TypeTag](other: DataSetAPI[U], condition: Expressions.BooleanExpression): DataSetAPI[(T, U)] =
    create(data.joinWith(other.toDataset, condition.toSparkColumn, "outer"))

  override def write: WriterExecutor[T] = new WriterSparkImpl[T].write(this)

}
