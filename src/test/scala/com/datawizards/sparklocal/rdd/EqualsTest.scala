package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EqualsTest extends SparkLocalBaseTest {

  test("equals") {
    assert(RDDAPI(Seq(1,2,3)) == RDDAPI(Seq(1,2,3)))
  }

  test("not equals") {
    assert(RDDAPI(Seq(1,2,3)) != RDDAPI(Seq(1,2)))
  }

  test("not equals - wrong type") {
    assert(RDDAPI(Seq(1,2,3)) != 1)
  }

}