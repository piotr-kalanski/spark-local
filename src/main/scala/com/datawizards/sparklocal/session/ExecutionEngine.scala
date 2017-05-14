package com.datawizards.sparklocal.session


object ExecutionEngine extends Enumeration {

  /**
    * Internal execution engine of all operations e.g. Spark, Scala
    */
  type ExecutionEngine = Value

  /**
    *  Spark execution engine
    */
  val Spark = Value

  /**
    * Scala collections with eager transformations
    */
  val ScalaEager = Value
}
