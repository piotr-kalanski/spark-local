package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class ForeachTest extends SparkLocalBaseTest {

  test("Foreach - Scala") {
    val buff1 = new ListBuffer[Int]
    RDDAPI(Seq(1,2,3)).foreach(x => buff1 += x)
    assert(buff1.toList == List(1,2,3))
  }

  test("Foreach partition - Scala") {
    val buff1 = new ListBuffer[Int]
    RDDAPI(Seq(1,2,3)).foreachPartition(x => buff1 ++= x)
    assert(buff1.toList == List(1,2,3))
  }

}