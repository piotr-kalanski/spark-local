package com.datawizards.sparklocal.impl.spark.accumulator

import java.lang

import com.datawizards.sparklocal.accumulator.{AccumulatorV2API, LongAccumulatorAPI}
import org.apache.spark.util.LongAccumulator

class LongAccumulatorAPISparkImpl(acc: LongAccumulator)
  extends AccumulatorV2APISparkImpl[java.lang.Long, java.lang.Long](acc)
  with LongAccumulatorAPI {

  override def copy(): AccumulatorV2API[lang.Long, lang.Long] =
    new LongAccumulatorAPISparkImpl(acc.copy())

  override def merge(other: AccumulatorV2API[lang.Long, lang.Long]): Unit = other match {
    case a:LongAccumulatorAPISparkImpl => acc.merge(a.acc)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def add(v: Long): Unit = acc.add(v)

  override def count: Long = acc.count

  override def sum: Long = acc.sum

  override def avg: Double = acc.avg
}
