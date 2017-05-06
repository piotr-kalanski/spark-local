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

  override def keys: RDDAPI[K] =
    rdd.map(_._1)

  override def values: RDDAPI[V] =
    rdd.map(_._2)

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

  override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], partitioner: Partitioner): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    cogroup(other1, other2, other3)

  override def cogroup[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Iterable[V], Iterable[W]))] =
    cogroup(other)

  override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], partitioner: Partitioner): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    cogroup(other1, other2)

  override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)]): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = (other1, other2, other3) match {
    case (rddScala1:RDDAPIScalaImpl[(K, W1)], rddScala2:RDDAPIScalaImpl[(K, W2)], rddScala3:RDDAPIScalaImpl[(K, W3)]) => RDDAPI({
      val xs      = data.groupBy(_._1).mapValues(_.map(_._2))
      val other1s      = rddScala1.data.groupBy(_._1).mapValues(_.map(_._2))
      val other2s      = rddScala2.data.groupBy(_._1).mapValues(_.map(_._2))
      val other3s      = rddScala3.data.groupBy(_._1).mapValues(_.map(_._2))
      val allKeys = xs.keys ++ other1s.keys ++ other2s.keys ++ other3s.keys
      allKeys.map { key =>
        val xsWithKey = xs.getOrElse(key, Iterable.empty)
        val other1WithKey = other1s.getOrElse(key, Iterable.empty)
        val other2WithKey = other2s.getOrElse(key, Iterable.empty)
        val other3WithKey = other3s.getOrElse(key, Iterable.empty)
        key -> (xsWithKey, other1WithKey, other2WithKey, other3WithKey)
      }
    })
    case _ => RDDAPI(rdd.toRDD.cogroup(other1.toRDD, other2.toRDD, other3.toRDD))
  }

  override def cogroup[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (Iterable[V], Iterable[W]))] = other match {
    case rddScala:RDDAPIScalaImpl[(K, W)] => RDDAPI({
      val xs      = data.groupBy(_._1).mapValues(_.map(_._2))
      val ys      = rddScala.data.groupBy(_._1).mapValues(_.map(_._2))
      val allKeys = xs.keys ++ ys.keys
      allKeys.map { key =>
        val xsWithKey = xs.getOrElse(key, Iterable.empty)
        val ysWithKey = ys.getOrElse(key, Iterable.empty)
        key -> (xsWithKey, ysWithKey)
      }
    })
    case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).cogroup(rddSpark.data))
  }

  override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)]): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = (other1, other2) match {
    case (rddScala1:RDDAPIScalaImpl[(K, W1)], rddScala2:RDDAPIScalaImpl[(K, W2)]) => RDDAPI({
      val xs      = data.groupBy(_._1).mapValues(_.map(_._2))
      val other1s      = rddScala1.data.groupBy(_._1).mapValues(_.map(_._2))
      val other2s      = rddScala2.data.groupBy(_._1).mapValues(_.map(_._2))
      val allKeys = xs.keys ++ other1s.keys ++ other2s.keys
      allKeys.map { key =>
        val xsWithKey = xs.getOrElse(key, Iterable.empty)
        val other1WithKey = other1s.getOrElse(key, Iterable.empty)
        val other2WithKey = other2s.getOrElse(key, Iterable.empty)
        key -> (xsWithKey, other1WithKey, other2WithKey)
      }
    })
    case _ => RDDAPI(rdd.toRDD.cogroup(other1.toRDD, other2.toRDD))
  }

  override def cogroup[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Iterable[V], Iterable[W]))] =
    cogroup(other)

  override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], numPartitions: Int): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] =
    cogroup(other1, other2)

  override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], numPartitions: Int): RDDAPI[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] =
    cogroup(other1, other2, other3)

  override def collectAsMap(): Map[K, V] = data.toMap

  override def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, V)] = other match {
    case rddScala: RDDAPIScalaImpl[(K, W)] =>
      val otherSet = rddScala.data.map{case (k,_) => k}.toSet
      RDDAPI(data.filter{case (k,_) => !otherSet.contains(k)})
    case rddSpark: RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).subtractByKey(rddSpark.data))
  }

  override def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, V)] = subtractByKey(other)

  override def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], p: Partitioner): RDDAPI[(K, V)] = subtractByKey(other)

  override def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)] =
    RDDAPI(
      data
        .groupBy(_._1)
        .mapValues(
          _.map(_._2)
            .aggregate(zeroValue)(seqOp, combOp)
        )
    )

  override def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)] =
    aggregateByKey(zeroValue)(seqOp, combOp)

  override def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)] =
    aggregateByKey(zeroValue)(seqOp, combOp)

}
