package com.datawizards.sparklocal.accumulator

trait DoubleAccumulatorAPI extends AccumulatorV2API[java.lang.Double, java.lang.Double] {
  /**
    * Adds v to the accumulator, i.e. increment sum by v and count by 1.
    */
  def add(v: Double): Unit

  /**
    * Returns the number of elements added to the accumulator.
    */
  def count: Long

  /**
    * Returns the sum of elements added to the accumulator.
    */
  def sum: Double

  /**
    * Returns the average of elements added to the accumulator.
    */
  def avg: Double
}
