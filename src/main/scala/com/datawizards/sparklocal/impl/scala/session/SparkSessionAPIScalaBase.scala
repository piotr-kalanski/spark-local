package com.datawizards.sparklocal.impl.scala.session

import com.datawizards.sparklocal.accumulator.{AccumulatorV2API, CollectionAccumulatorAPI, DoubleAccumulatorAPI, LongAccumulatorAPI}
import com.datawizards.sparklocal.broadcast.BroadcastAPI
import com.datawizards.sparklocal.impl.scala.accumulator.{CollectionAccumulatorAPIScalaImpl, DoubleAccumulatorAPIScalaImpl, LongAccumulatorAPIScalaImpl}
import com.datawizards.sparklocal.impl.scala.broadcast.BroadcastAPIScalaImpl
import com.datawizards.sparklocal.session.SparkSessionAPI
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

trait SparkSessionAPIScalaBase extends SparkSessionAPI {

  object implicits {
    implicit def enc[T]: Encoder[T] = null
  }

  override def broadcast[T: ClassTag](value: T): BroadcastAPI[T] =
    new BroadcastAPIScalaImpl[T](value)

  override def longAccumulator: LongAccumulatorAPI =
    new LongAccumulatorAPIScalaImpl()

  override def longAccumulator(name: String): LongAccumulatorAPI =
    new LongAccumulatorAPIScalaImpl(Some(name))

  override def doubleAccumulator: DoubleAccumulatorAPI =
    new DoubleAccumulatorAPIScalaImpl()

  override def doubleAccumulator(name: String): DoubleAccumulatorAPI =
    new DoubleAccumulatorAPIScalaImpl(Some(name))

  override def collectionAccumulator[T]: CollectionAccumulatorAPI[T] =
    new CollectionAccumulatorAPIScalaImpl[T]()

  override def collectionAccumulator[T](name: String): CollectionAccumulatorAPI[T] =
    new CollectionAccumulatorAPIScalaImpl[T](Some(name))

  override def register(acc: AccumulatorV2API[_, _], name: String): Unit =
    { /* do nothing */ }

  override def register(acc: AccumulatorV2API[_, _]): Unit =
    { /* do nothing */ }
}
