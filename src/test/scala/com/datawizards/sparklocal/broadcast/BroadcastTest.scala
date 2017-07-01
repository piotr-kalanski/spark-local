package com.datawizards.sparklocal.broadcast

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import org.apache.spark.SparkException
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BroadcastTest extends SparkLocalBaseTest {
  test("broadcast") {
    testBroadcast(ExecutionEngine.ScalaEager)
    testBroadcast(ExecutionEngine.ScalaLazy)
    testBroadcast(ExecutionEngine.ScalaParallel)
    testBroadcast(ExecutionEngine.ScalaParallelLazy)
    testBroadcast(ExecutionEngine.Spark)
  }

  test("broadcast with unpersist(true)") {
    testBroadcast(ExecutionEngine.ScalaEager, Some(true))
    testBroadcast(ExecutionEngine.ScalaLazy, Some(true))
    testBroadcast(ExecutionEngine.ScalaParallel, Some(true))
    testBroadcast(ExecutionEngine.ScalaParallelLazy, Some(true))
    testBroadcast(ExecutionEngine.Spark, Some(true))
  }

  test("broadcast with unpersist(false)") {
    testBroadcast(ExecutionEngine.ScalaEager, Some(false))
    testBroadcast(ExecutionEngine.ScalaLazy, Some(false))
    testBroadcast(ExecutionEngine.ScalaParallel, Some(false))
    testBroadcast(ExecutionEngine.ScalaParallelLazy, Some(false))
    testBroadcast(ExecutionEngine.Spark, Some(false))
  }

  test("broadcast.destroy - Scala - nothing happens") {
    val session = SparkSessionAPI.builder(ExecutionEngine.ScalaEager).master("local").getOrCreate()
    val b = session.broadcast(1)
    val rdd = session.createRDD(Seq(1,2,3))
    b.destroy()
    assertRDDOperationResultWithSorted(rdd.map(x => x + b.value)) {
      Array(2,3,4)
    }
  }

  test("broadcast.destroy - Spark - exception") {
    val session = SparkSessionAPI.builder(ExecutionEngine.Spark).master("local").getOrCreate()
    val b = session.broadcast(1)
    val rdd = session.createRDD(Seq(1,2,3))
    b.destroy()
    intercept[SparkException] {
      rdd.map(x => x + b.value).collect()
    }
  }

  private def testBroadcast[S <: SparkSessionAPI](executionEngine: ExecutionEngine[S], unpersist: Option[Boolean] = None): Unit = {
    val session = SparkSessionAPI.builder(executionEngine).master("local").getOrCreate()
    val b = session.broadcast(1)
    if(unpersist.isDefined)
      if(unpersist.get)
        b.unpersist(unpersist.get)
      else
        b.unpersist()
    val rdd = session.createRDD(Seq(1,2,3))
    assertRDDOperationResultWithSorted(rdd.map(x => x + b.value)) {
      Array(2,3,4)
    }
  }
}
