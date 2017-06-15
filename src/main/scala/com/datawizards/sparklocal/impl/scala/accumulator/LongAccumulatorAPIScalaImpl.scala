package com.datawizards.sparklocal.impl.scala.accumulator

import java.lang

import com.datawizards.sparklocal.accumulator.{AccumulatorV2API, LongAccumulatorAPI}

class LongAccumulatorAPIScalaImpl(name: Option[String]=None)
  extends AccumulatorV2APIScalaImpl[java.lang.Long, java.lang.Long](name)
    with LongAccumulatorAPI {

  private var _sum = 0L
  private var _count = 0L

  override def isZero: Boolean = _sum == 0L && _count == 0

  override def copy(): AccumulatorV2API[lang.Long, lang.Long] = {
    val newAcc = new LongAccumulatorAPIScalaImpl
    newAcc._count = this._count
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0L
    _count = 0L
  }

  override def add(v: lang.Long): Unit = {
    _sum += v
    _count += 1
  }

  override def merge(other: AccumulatorV2API[lang.Long, lang.Long]): Unit = other match {
    case a:LongAccumulatorAPIScalaImpl =>
      _sum += a._sum
      _count += a._count
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: lang.Long = _sum

  override def add(v: Long): Unit = {
    _sum += v
    _count += 1
  }

  override def count: Long = _count

  override def sum: Long = _sum

  override def avg: Double = _sum.toDouble / _count
}
