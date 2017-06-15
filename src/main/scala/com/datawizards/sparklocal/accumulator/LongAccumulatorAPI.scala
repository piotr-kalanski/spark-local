package com.datawizards.sparklocal.accumulator

trait LongAccumulatorAPI extends AccumulatorV2API[java.lang.Long, java.lang.Long] {
  /**
    * Adds v to the accumulator, i.e. increment sum by v and count by 1.
    */
  def add(v: Long): Unit

  /**
    * Returns the number of elements added to the accumulator.
    */
  def count: Long

  /**
    * Returns the sum of elements added to the accumulator.
    */
  def sum: Long

  /**
    * Returns the average of elements added to the accumulator.
    */
  def avg: Double
}
