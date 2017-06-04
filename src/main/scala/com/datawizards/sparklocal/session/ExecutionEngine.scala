package com.datawizards.sparklocal.session

import com.datawizards.sparklocal.impl.scala.eager.session.{BuilderScalaImpl, SparkSessionAPIScalaImpl}
import com.datawizards.sparklocal.impl.spark.session.{BuilderSparkImpl, SparkSessionAPISparkImpl}
import org.apache.spark.sql.SparkSession

/**
  * Internal execution engine of all operations e.g. Spark, Scala
  */
trait ExecutionEngine[Session <: SparkSessionAPI] {
  def builder(): Builder[Session]
}

object ExecutionEngine  {

  /**
    *  Spark execution engine
    */
  object Spark extends ExecutionEngine[SparkSessionAPISparkImpl] {
    override def builder(): Builder[SparkSessionAPISparkImpl] = new BuilderSparkImpl(SparkSession.builder())
  }

  /**
    * Scala collections with eager transformations
    */
  object ScalaEager extends ExecutionEngine[SparkSessionAPIScalaImpl] {
    override def builder(): Builder[SparkSessionAPIScalaImpl] = new BuilderScalaImpl
  }

}
