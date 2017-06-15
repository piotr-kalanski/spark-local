package com.datawizards.sparklocal.impl.spark.accumulator

import com.datawizards.sparklocal.accumulator.{AccumulatorV2API, DoubleAccumulatorAPI}
import org.apache.spark.util.DoubleAccumulator

class DoubleAccumulatorAPISparkImpl(acc: DoubleAccumulator)
  extends AccumulatorV2APISparkImpl[java.lang.Double, java.lang.Double](acc)
    with DoubleAccumulatorAPI {

  override def copy(): AccumulatorV2API[java.lang.Double, java.lang.Double] =
    new DoubleAccumulatorAPISparkImpl(acc.copy())

  override def merge(other: AccumulatorV2API[java.lang.Double, java.lang.Double]): Unit = other match {
    case a:DoubleAccumulatorAPISparkImpl => acc.merge(a.acc)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def add(v: Double): Unit = acc.add(v)

  override def count: Long = acc.count

  override def sum: Double = acc.sum

  override def avg: Double = acc.avg
}
