package com.datawizards.sparklocal.accumulator

trait AccumulatorV2API[IN, OUT] extends Serializable {
  /**
    * Returns true if this accumulator has been registered.
    *
    * @note All accumulators must be registered before use, or it will throw exception.
    */
  def isRegistered: Boolean

  /**
    * Returns the id of this accumulator, can only be called after registration.
    */
  def id: Long

  /**
    * Returns the name of this accumulator, can only be called after registration.
    */
  def name: Option[String]

  /**
    * Returns if this accumulator is zero value or not. e.g. for a counter accumulator, 0 is zero
    * value; for a list accumulator, Nil is zero value.
    */
  def isZero: Boolean

  /**
    * Creates a new copy of this accumulator, which is zero value. i.e. call `isZero` on the copy
    * must return true.
    */
  def copyAndReset(): AccumulatorV2API[IN, OUT]

  /**
    * Creates a new copy of this accumulator.
    */
  def copy(): AccumulatorV2API[IN, OUT]

  /**
    * Resets this accumulator, which is zero value. i.e. call `isZero` must
    * return true.
    */
  def reset(): Unit

  /**
    * Takes the inputs and accumulates.
    */
  def add(v: IN): Unit

  /**
    * Merges another same-type accumulator into this one and update its state, i.e. this should be
    * merge-in-place.
    */
  def merge(other: AccumulatorV2API[IN, OUT]): Unit

  /**
    * Defines the current value of this accumulator
    */
  def value: OUT

}
