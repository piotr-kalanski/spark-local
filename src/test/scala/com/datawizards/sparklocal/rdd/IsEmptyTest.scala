package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IsEmptyTest extends SparkLocalBaseTest {

  test("isEmpty result") {
    assert(!RDDAPI(Seq(1,2,3)).isEmpty)
    assert(RDDAPI(Seq()).isEmpty)
  }

  test("isEmpty equal") {
    assertRDDOperation[Int, Boolean](Seq(1,2,3)) {
      d => d.isEmpty
    }
    assertRDDOperation[Int, Boolean](Seq()) {
      d => d.isEmpty
    }
  }

}