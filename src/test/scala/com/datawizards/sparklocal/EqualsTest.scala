package com.datawizards.sparklocal

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EqualsTest extends SparkLocalBaseTest {

  test("equals result") {
      assert(DataSetAPI(Seq(1,2,3)) == DataSetAPI(Seq(1,2,3)))
      assert(DataSetAPI(Seq(1,2,3)) != 1)
  }

}