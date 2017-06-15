package com.datawizards.sparklocal.impl.scala.accumulator

import com.datawizards.sparklocal.accumulator.{AccumulatorV2API, DoubleAccumulatorAPI}

class DoubleAccumulatorAPIScalaImpl(name: Option[String]=None)
  extends AccumulatorV2APIScalaImpl[java.lang.Double, java.lang.Double](name)
    with DoubleAccumulatorAPI {

  private var _sum = 0.0
  private var _count = 0L

  override def isZero: Boolean = _sum == 0.0 && _count == 0

  override def copy(): DoubleAccumulatorAPI = {
    val newAcc = new DoubleAccumulatorAPIScalaImpl
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0.0
    _count = 0L
  }

  override def add(v: java.lang.Double): Unit = {
    _sum += v
    _count += 1
  }

  override def add(v: Double): Unit = {
    _sum += v
    _count += 1
  }

  override def count: Long = _count

  override def sum: Double = _sum

  override def avg: Double = _sum / _count

  override def merge(other: AccumulatorV2API[java.lang.Double, java.lang.Double]): Unit = other match {
    case o: DoubleAccumulatorAPIScalaImpl =>
      _sum += o.sum
      _count += o.count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: java.lang.Double = _sum
}
