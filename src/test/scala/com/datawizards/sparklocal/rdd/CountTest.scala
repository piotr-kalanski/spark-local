package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CountTest extends SparkLocalBaseTest {

  test("Count result") {
    assert(RDDAPI(Seq(1,2,3)).count == 3)
  }

  test("Count equal") {
    assertRDDOperation(Seq(1,2,3)) {
      ds => ds.count()
    }
  }

}
