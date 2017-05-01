package com.datawizards.sparklocal.rdd

import org.apache.spark.Partitioner
import scala.reflect.ClassTag
import scala.collection.Map
import scala.collection.mutable.ListBuffer

class PairRDDFunctionsAPIScalaImpl[K,V](rdd: RDDAPIScalaImpl[(K,V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctionsAPI[K,V]
{
  private val data = rdd.data

  override def mapValues[U](f: (V) => U): RDDAPI[(K, U)] =
    RDDAPI(
      data.map { case (k, v) => (k, f(v)) }
    )

  override def keys: RDDAPI[K] = rdd.map(_._1)

  override def values: RDDAPI[V] = rdd.map(_._2)

  override def flatMapValues[U](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)] =
    rdd.flatMap { case (k,v) =>
      f(v).map(x => (k,x))
    }

  override def countByKey(): Map[K, Long] =
    data
      .groupBy(_._1)
      .mapValues(_.size)

  override def reduceByKey(func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(
      data
        .groupBy(_._1)
        .mapValues(
          _.map(_._2)
            .reduce(func)
        )
    )

  override def reduceByKey(func: (V, V) => V, numPartitions: Int): RDDAPI[(K, V)] = reduceByKey(func)

  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDDAPI[(K, V)] = reduceByKey(func)

  override def reduceByKeyLocally(func: (V, V) => V): Map[K, V] =
    reduceByKey(func).collect().toMap

  override def groupByKey(): RDDAPI[(K, Iterable[V])] =
    RDDAPI(
      data
        .groupBy(_._1)
        .mapValues(_.map(_._2))
    )

  override def groupByKey(numPartitions: Int): RDDAPI[(K, Iterable[V])] = groupByKey()

  override def groupByKey(partitioner: Partitioner): RDDAPI[(K, Iterable[V])] = groupByKey()

  override def foldByKey(zeroValue: V)(func: (V, V) => V): RDDAPI[(K, V)] =
    RDDAPI(
      data
        .groupBy(_._1)
        .mapValues(
          _.map(_._2)
            .fold(zeroValue)(func)
        )
    )

  override def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDDAPI[(K, V)] = foldByKey(zeroValue)(func)

  override def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDDAPI[(K, V)] = foldByKey(zeroValue)(func)

  override def join[W](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, W))] = other match {
    case rddScala:RDDAPIScalaImpl[(K, W)] => RDDAPI(
      for {
        left <- data
        right <- rddScala.data
        if left._1 == right._1
      } yield (left._1, (left._2, right._2))
    )
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).join(rddSpark.data))
  }

  override def join[W](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, W))] = other match {
    case _:RDDAPIScalaImpl[(K, W)] => join(other)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).join(rddSpark.data, numPartitions))
  }

  override def join[W](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, W))] = other match {
    case _:RDDAPIScalaImpl[(K, W)] => join(other)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).join(rddSpark.data, partitioner))
  }

  override def leftOuterJoin[W](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, Option[W]))] = other match {
    case rddScala:RDDAPIScalaImpl[(K, W)] =>
      val b = new ListBuffer[(K, (V, Option[W]))]

      for (left <- data) {
        var rightExists = false
        for (right <- rddScala.data) {
          if (left._1 == right._1) {
            b += ((left._1, (left._2, Some(right._2))))
            rightExists = true
          }
        }
        if(!rightExists) {
          b += ((left._1, (left._2, None)))
        }
      }

      RDDAPI(b)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).leftOuterJoin(rddSpark.data))
  }

  override def leftOuterJoin[W](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, Option[W]))] = other match {
    case _:RDDAPIScalaImpl[(K, W)] => leftOuterJoin(other)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).leftOuterJoin(rddSpark.data, numPartitions))
  }

  override def leftOuterJoin[W](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, Option[W]))] = other match {
    case _:RDDAPIScalaImpl[(K, W)] => leftOuterJoin(other)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).leftOuterJoin(rddSpark.data, partitioner))
  }

  override def rightOuterJoin[W](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], W))] = other match {
    case rddScala:RDDAPIScalaImpl[(K, W)] =>
      val b = new ListBuffer[(K, (Option[V], W))]

      for (right <- rddScala.data) {
        var leftExists = false
        for (left <- data) {
          if (left._1 == right._1) {
            b += ((right._1, (Some(left._2), right._2)))
            leftExists = true
          }
        }
        if(!leftExists) {
          b += ((right._1, (None, right._2)))
        }
      }

      RDDAPI(b)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).rightOuterJoin(rddSpark.data))
  }

  override def rightOuterJoin[W](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], W))] = other match {
    case _:RDDAPIScalaImpl[(K, W)] => rightOuterJoin(other)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).rightOuterJoin(rddSpark.data, numPartitions))
  }

  override def rightOuterJoin[W](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], W))] = other match {
    case _:RDDAPIScalaImpl[(K, W)] => rightOuterJoin(other)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).rightOuterJoin(rddSpark.data, partitioner))
  }

  override def fullOuterJoin[W](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], Option[W]))] = other match {
    case rddScala:RDDAPIScalaImpl[(K, W)] =>
      val b = new ListBuffer[(K, (Option[V], Option[W]))]

      for (left <- data) {
        var rightExists = false
        for (right <- rddScala.data) {
          if (left._1 == right._1) {
            b += ((right._1, (Some(left._2), Some(right._2))))
            rightExists = true
          }
        }
        if(!rightExists) {
          b += ((left._1, (Some(left._2), None)))
        }
      }
      for (right <- rddScala.data) {
        var leftExists = false
        for (left <- data) {
          if (left._1 == right._1) {
            leftExists = true
          }
        }
        if(!leftExists) {
          b += ((right._1, (None, Some(right._2))))
        }
      }

      RDDAPI(b)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).fullOuterJoin(rddSpark.data))
  }

  override def fullOuterJoin[W](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], Option[W]))] = other match {
    case _:RDDAPIScalaImpl[(K, W)] => fullOuterJoin(other)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).fullOuterJoin(rddSpark.data, numPartitions))
  }

  override def fullOuterJoin[W](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], Option[W]))] = other match {
    case _:RDDAPIScalaImpl[(K, W)] => fullOuterJoin(other)
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).fullOuterJoin(rddSpark.data, partitioner))
  }

}
