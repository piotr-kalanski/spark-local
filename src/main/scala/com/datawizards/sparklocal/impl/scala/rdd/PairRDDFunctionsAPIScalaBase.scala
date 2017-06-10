package com.datawizards.sparklocal.impl.scala.rdd

import com.datawizards.sparklocal.impl.spark.rdd.RDDAPISparkImpl
import com.datawizards.sparklocal.rdd.{PairRDDFunctionsAPI, RDDAPI}
import org.apache.spark.Partitioner

import scala.collection.{GenIterable, GenMap, Map}
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

abstract class PairRDDFunctionsAPIScalaBase[K, V](implicit kct: ClassTag[K], vct: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctionsAPI[K,V]
  {

    protected val rdd: RDDAPIScalaBase[(K,V)]

    private val data = rdd.data

    protected def create[U: ClassTag](data: GenIterable[U]): RDDAPIScalaBase[U]

    override def mapValues[U: ClassTag](f: (V) => U): RDDAPI[(K, U)] =
      create(
        data.map { case (k, v) => (k, f(v)) }
      )

    override def keys: RDDAPI[K] =
      rdd.map(_._1)

    override def values: RDDAPI[V] =
      rdd.map(_._2)

    override def flatMapValues[U: ClassTag](f: (V) => TraversableOnce[U]): RDDAPI[(K, U)] =
      rdd.flatMap { case (k,v) =>
        f(v).map(x => (k,x))
      }

    override def countByKey(): GenMap[K, Long] =
      data
        .groupBy(_._1)
        .mapValues(_.size)

    override def reduceByKey(func: (V, V) => V): RDDAPI[(K, V)] =
      create(
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

    override def groupByKey(): RDDAPI[(K, GenIterable[V])] =
      create(
        data
          .groupBy(_._1)
          .mapValues(_.map(_._2))
      )

    override def groupByKey(numPartitions: Int): RDDAPI[(K, GenIterable[V])] = groupByKey()

    override def groupByKey(partitioner: Partitioner): RDDAPI[(K, GenIterable[V])] = groupByKey()

    override def foldByKey(zeroValue: V)(func: (V, V) => V): RDDAPI[(K, V)] =
      create(
        data
          .groupBy(_._1)
          .mapValues(
            _.map(_._2)
              .fold(zeroValue)(func)
          )
      )

    override def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDDAPI[(K, V)] = foldByKey(zeroValue)(func)

    override def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): RDDAPI[(K, V)] = foldByKey(zeroValue)(func)

    override def join[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, W))] = other match {
      case rddScala:RDDAPIScalaBase[(K, W)] =>
        this.cogroup(other).flatMapValues( pair =>
          for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w)
        )
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).join(rddSpark.data))
    }

    override def join[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, W))] = other match {
      case _:RDDAPIScalaBase[(K, W)] => join(other)
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).join(rddSpark.data, numPartitions))
    }

    override def join[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, W))] = other match {
      case _:RDDAPIScalaBase[(K, W)] => join(other)
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).join(rddSpark.data, partitioner))
    }

    override def leftOuterJoin[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (V, Option[W]))] = other match {
      case rddScala:RDDAPIScalaBase[(K, W)] =>
        this.cogroup(other).flatMapValues { pair =>
          if (pair._2.isEmpty) {
            pair._1.iterator.map(v => (v, None))
          } else {
            for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, Some(w))
          }
        }
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).leftOuterJoin(rddSpark.data))
    }

    override def leftOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (V, Option[W]))] = other match {
      case _:RDDAPIScalaBase[(K, W)] => leftOuterJoin(other)
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).leftOuterJoin(rddSpark.data, numPartitions))
    }

    override def leftOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (V, Option[W]))] = other match {
      case _:RDDAPIScalaBase[(K, W)] => leftOuterJoin(other)
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).leftOuterJoin(rddSpark.data, partitioner))
    }

    override def rightOuterJoin[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], W))] = other match {
      case rddScala:RDDAPIScalaBase[(K, W)] =>
        this.cogroup(other).flatMapValues { pair =>
          if (pair._1.isEmpty) {
            pair._2.iterator.map(w => (None, w))
          } else {
            for (v <- pair._1.iterator; w <- pair._2.iterator) yield (Some(v), w)
          }
        }
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).rightOuterJoin(rddSpark.data))
    }

    override def rightOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], W))] = other match {
      case _:RDDAPIScalaBase[(K, W)] => rightOuterJoin(other)
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).rightOuterJoin(rddSpark.data, numPartitions))
    }

    override def rightOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], W))] = other match {
      case _:RDDAPIScalaBase[(K, W)] => rightOuterJoin(other)
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).rightOuterJoin(rddSpark.data, partitioner))
    }

    override def fullOuterJoin[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (Option[V], Option[W]))] = other match {
      case rddScala:RDDAPIScalaBase[(K, W)] =>
        this.cogroup(other).flatMapValues {
          case (vs, Seq()) => vs.iterator.map(v => (Some(v), None))
          case (Seq(), ws) => ws.iterator.map(w => (None, Some(w)))
          case (vs, ws) => for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), Some(w))
        }
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).fullOuterJoin(rddSpark.data))
    }

    override def fullOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (Option[V], Option[W]))] = other match {
      case _:RDDAPIScalaBase[(K, W)] => fullOuterJoin(other)
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).fullOuterJoin(rddSpark.data, numPartitions))
    }

    override def fullOuterJoin[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (Option[V], Option[W]))] = other match {
      case _:RDDAPIScalaBase[(K, W)] => fullOuterJoin(other)
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).fullOuterJoin(rddSpark.data, partitioner))
    }

    override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], partitioner: Partitioner): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2], GenIterable[W3]))] =
      cogroup(other1, other2, other3)

    override def cogroup[W: ClassTag](other: RDDAPI[(K, W)], partitioner: Partitioner): RDDAPI[(K, (GenIterable[V], GenIterable[W]))] =
      cogroup(other)

    override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], partitioner: Partitioner): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2]))] =
      cogroup(other1, other2)

    override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)]): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2], GenIterable[W3]))] = (other1, other2, other3) match {
      case (rddScala1:RDDAPIScalaBase[(K, W1)], rddScala2:RDDAPIScalaBase[(K, W2)], rddScala3:RDDAPIScalaBase[(K, W3)]) => create({
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
      case _ => RDDAPI(rdd.toRDD).cogroup(RDDAPI(other1.toRDD), RDDAPI(other2.toRDD), RDDAPI(other3.toRDD))
    }

    override def cogroup[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, (GenIterable[V], GenIterable[W]))] = other match {
      case rddScala:RDDAPIScalaBase[(K, W)] => create({
        val xs      = data.groupBy(_._1).mapValues(_.map(_._2))
        val ys      = rddScala.data.groupBy(_._1).mapValues(_.map(_._2))
        val allKeys = xs.keys ++ ys.keys
        allKeys.map { key =>
          val xsWithKey = xs.getOrElse(key, Iterable.empty)
          val ysWithKey = ys.getOrElse(key, Iterable.empty)
          key -> (xsWithKey, ysWithKey)
        }
      })
      case rddSpark:RDDAPISparkImpl[(K, W)] => RDDAPI(rdd.toRDD).cogroup(rddSpark)
    }

    override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)]): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2]))] = (other1, other2) match {
      case (rddScala1:RDDAPIScalaBase[(K, W1)], rddScala2:RDDAPIScalaBase[(K, W2)]) => create({
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
      case _ => RDDAPI(rdd.toRDD).cogroup(RDDAPI(other1.toRDD), RDDAPI(other2.toRDD))
    }

    override def cogroup[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, (GenIterable[V], GenIterable[W]))] =
      cogroup(other)

    override def cogroup[W1: ClassTag, W2: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], numPartitions: Int): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2]))] =
      cogroup(other1, other2)

    override def cogroup[W1: ClassTag, W2: ClassTag, W3: ClassTag](other1: RDDAPI[(K, W1)], other2: RDDAPI[(K, W2)], other3: RDDAPI[(K, W3)], numPartitions: Int): RDDAPI[(K, (GenIterable[V], GenIterable[W1], GenIterable[W2], GenIterable[W3]))] =
      cogroup(other1, other2, other3)

    override def collectAsMap(): GenMap[K, V] = data.toMap

    override def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)]): RDDAPI[(K, V)] = other match {
      case rddScala: RDDAPIScalaBase[(K, W)] =>
        val otherSet = rddScala.data.map{case (k,_) => k}.toSet
        create(data.filter{case (k,_) => !otherSet.contains(k)})
      case rddSpark: RDDAPISparkImpl[(K, W)] => RDDAPI(parallelize(data).subtractByKey(rddSpark.data))
    }

    override def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], numPartitions: Int): RDDAPI[(K, V)] = subtractByKey(other)

    override def subtractByKey[W: ClassTag](other: RDDAPI[(K, W)], p: Partitioner): RDDAPI[(K, V)] = subtractByKey(other)

    override def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDDAPI[(K, U)] =
      create(
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

    override def partitionBy(partitioner: Partitioner): RDDAPI[(K, V)] =
      rdd

  }
