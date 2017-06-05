package com.datawizards.sparklocal.session

import com.datawizards.sparklocal.impl.scala.`lazy`.session.{BuilderScalaLazyImpl, SparkSessionAPIScalaLazyImpl}
import com.datawizards.sparklocal.impl.scala.eager.session.{BuilderScalaEagerImpl, SparkSessionAPIScalaEagerImpl}
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
  object ScalaEager extends ExecutionEngine[SparkSessionAPIScalaEagerImpl] {
    override def builder(): Builder[SparkSessionAPIScalaEagerImpl] = new BuilderScalaEagerImpl
  }

  /**
    * Scala collections with lazy transformations
    */
  object ScalaLazy extends ExecutionEngine[SparkSessionAPIScalaLazyImpl] {
    override def builder(): Builder[SparkSessionAPIScalaLazyImpl] = new BuilderScalaLazyImpl
  }

}
