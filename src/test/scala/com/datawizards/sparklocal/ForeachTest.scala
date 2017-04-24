package com.datawizards.sparklocal

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class ForeachTest extends SparkLocalBaseTest {

  test("Foreach") {
      val buff = new ListBuffer[Int]
      DataSetAPI(Seq(1,2,3)).foreach(x => buff += x)
      assert(buff.toList == List(1,2,3))
  }

}