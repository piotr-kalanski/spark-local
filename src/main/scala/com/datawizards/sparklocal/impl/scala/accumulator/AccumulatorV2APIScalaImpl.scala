package com.datawizards.sparklocal.impl.scala.accumulator

import java.util.concurrent.atomic.AtomicLong

import com.datawizards.sparklocal.accumulator.AccumulatorV2API

object AccumulatorV2APIScalaImpl {
  private val nextId = new AtomicLong
  def newId(): Long = nextId.getAndIncrement
}

abstract class AccumulatorV2APIScalaImpl[IN, OUT](accName: Option[String]) extends AccumulatorV2API[IN, OUT] {

  def isRegistered: Boolean = true

  def id: Long = AccumulatorV2APIScalaImpl.newId()

  def name: Option[String] = accName

  def copyAndReset(): AccumulatorV2API[IN, OUT] = {
    val copyAcc = copy()
    copyAcc.reset()
    copyAcc
  }

}

