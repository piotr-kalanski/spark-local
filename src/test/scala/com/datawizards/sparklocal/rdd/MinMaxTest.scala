package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MinMaxTest extends SparkLocalBaseTest {

  test("Min,max result") {
    assert(RDDAPI(Seq(1,2,3)).min == 1)
    assert(RDDAPI(Seq(1,2,3)).max == 3)
  }

  test("Min equal") {
    assertRDDOperation(Seq(1,2,3)) {
      ds => ds.min
    }
  }

  test("Max equal") {
    assertRDDOperation(Seq(1,2,3)) {
      ds => ds.max
    }
  }

}
