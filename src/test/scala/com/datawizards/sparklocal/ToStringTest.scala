package com.datawizards.sparklocal

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ToStringTest extends SparkLocalBaseTest {

  test("toString result") {
      assert(DataSetAPI(Seq(1,2,3)).toString == Array(1,2,3).toSeq.toString)
  }

  test("toString equal") {
    assertDatasetOperation(Seq(1,2,3)){
      ds => ds.toString
    }
  }

}