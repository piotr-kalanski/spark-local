package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class ForeachTest extends SparkLocalBaseTest {

  test("Foreach - Scala") {
    val buff1 = new ListBuffer[Int]
    DataSetAPI(Seq(1,2,3)).foreach(x => buff1 += x)
    assert(buff1.toList == List(1,2,3))
  }

  test("Foreach partition - Scala") {
    val buff1 = new ListBuffer[Int]
    DataSetAPI(Seq(1,2,3)).foreachPartition(x => buff1 ++= x)
    assert(buff1.toList == List(1,2,3))
  }

  test("Foreach - Spark") {
    import spark.implicits._
    var accum = sc.longAccumulator
    DataSetAPI(Seq(1,2,3).toDS).foreach(x => accum.add(x))
    assert(accum.value == 6L)
  }

  test("Foreach partition - Spark") {
    import spark.implicits._
    var accum = sc.longAccumulator
    DataSetAPI(Seq(1,2,3).toDS).foreachPartition(x => x.foreach(accum.add(_)))
    assert(accum.value == 6L)
  }

}