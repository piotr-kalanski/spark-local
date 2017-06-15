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
      assert(acc.id == 0L)
      assert(acc.isRegistered)
      assertResult(3L)(acc.count)
      assertResult(6L)(acc.sum)
      assertResult(6L)(acc.value)
      assertResult(2.0)(acc.avg)
      val copy = acc.copy().asInstanceOf[LongAccumulatorAPI]
      assert(acc.count == copy.count)
      assert(acc.sum == copy.sum)
      assert(acc.avg == copy.avg)
      val copy2 = acc.copyAndReset().asInstanceOf[LongAccumulatorAPI]
      assert(acc.count == copy.count)
      assert(acc.sum == copy.sum)
      assert(acc.avg == copy.avg)
      assert(copy2.count == 0L)
      assert(copy2.sum == 0L)
      copy.merge(copy)
      assertResult(6L)(copy.count)
      assertResult(12L)(copy.sum)
      assertResult(2.0)(copy.avg)
      copy.reset()
      assert(copy.count == 0L)
      assert(copy.sum == 0L)
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
      assert(acc.isRegistered)
      assertResult(3L)(acc.count)
      assertResult(6.0)(acc.sum)
      assertResult(6.0)(acc.value)
      assertResult(2.0)(acc.avg)
      val copy = acc.copy().asInstanceOf[DoubleAccumulatorAPI]
      assert(acc.count == copy.count)
      assert(acc.sum == copy.sum)
      assert(acc.avg == copy.avg)
      val copy2 = acc.copyAndReset().asInstanceOf[DoubleAccumulatorAPI]
      assert(acc.count == copy.count)
      assert(acc.sum == copy.sum)
      assert(acc.avg == copy.avg)
      assert(copy2.count == 0L)
      assert(copy2.sum == 0.0)
      copy.merge(copy)
      assertResult(6L)(copy.count)
      assertResult(12.0)(copy.sum)
      assertResult(2.0)(copy.avg)
      copy.reset()
      assert(copy.count == 0L)
      assert(copy.sum == 0.0)
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
      assert(acc.isRegistered)
      assertResult(java.util.Arrays.asList(1,2,3))(acc.value)
    }
    testEngine(ExecutionEngine.ScalaEager)
    testEngine(ExecutionEngine.Spark)
  }

  test("merge - exception") {
    val session1 = SparkSessionAPI.builder(ExecutionEngine.ScalaEager).master("local").getOrCreate()
    val session2 = SparkSessionAPI.builder(ExecutionEngine.Spark).master("local").getOrCreate()
    val accLong1 = session1.longAccumulator
    val accLong2 = session2.longAccumulator
    val accDouble1 = session1.doubleAccumulator
    val accDouble2 = session2.doubleAccumulator
    val accColl1 = session1.collectionAccumulator[Int]
    val accColl2 = session2.collectionAccumulator[Int]

    intercept[UnsupportedOperationException] {
      accLong1.merge(accLong2)
    }
    intercept[UnsupportedOperationException] {
      accDouble1.merge(accDouble2)
    }
    intercept[UnsupportedOperationException] {
      accColl1.merge(accColl2)
    }

  }

}

