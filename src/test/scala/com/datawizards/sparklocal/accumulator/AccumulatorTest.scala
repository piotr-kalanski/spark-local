package com.datawizards.sparklocal.accumulator

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumulatorTest extends SparkLocalBaseTest {

  test("long accumulator") {
    def testEngine[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): Unit = {
      val session = SparkSessionAPI.builder(engine).master("local").getOrCreate()
      val acc = session.longAccumulator
      assert(acc.isZero)
      val rdd = session.createRDD(Seq(1,2,3))
      rdd.foreach(x => acc.add(x))
      assert(!acc.isZero)
      assertResult(3L)(acc.count)
      assertResult(6L)(acc.sum)
      assertResult(2.0)(acc.avg)
    }
    testEngine(ExecutionEngine.ScalaEager)
    testEngine(ExecutionEngine.Spark)
  }

  test("double accumulator") {
    def testEngine[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): Unit = {
      val session = SparkSessionAPI.builder(engine).master("local").getOrCreate()
      val acc = session.doubleAccumulator
      assert(acc.isZero)
      val rdd = session.createRDD(Seq(1,2,3))
      rdd.foreach(x => acc.add(x))
      assert(!acc.isZero)
      assertResult(3L)(acc.count)
      assertResult(6.0)(acc.sum)
      assertResult(2.0)(acc.avg)
    }
    testEngine(ExecutionEngine.ScalaEager)
    testEngine(ExecutionEngine.Spark)
  }

  test("collection accumulator") {
    def testEngine[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): Unit = {
      val session = SparkSessionAPI.builder(engine).master("local").getOrCreate()
      val acc = session.collectionAccumulator[Int]
      assert(acc.isZero)
      val rdd = session.createRDD(Seq(1,2,3))
      rdd.foreach(x => acc.add(x))
      assert(!acc.isZero)
      assertResult(java.util.Arrays.asList(1,2,3))(acc.value)
    }
    testEngine(ExecutionEngine.ScalaEager)
    testEngine(ExecutionEngine.Spark)
  }

}
