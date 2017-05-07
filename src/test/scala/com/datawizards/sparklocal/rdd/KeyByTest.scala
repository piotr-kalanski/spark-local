package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KeyByTest extends SparkLocalBaseTest {

  val data = Seq(1,2,3,4,5)

  test("keyBy result") {
    assertRDDOperationResult(RDDAPI(data).keyBy(_ % 2)) {
      Array((1,1), (0,2), (1,3), (0,4), (1,5))
    }
  }

  test("keyBy equal") {
    assertRDDOperationReturnsSameResult(data) {
      ds => ds.keyBy(_ % 2)
    }
  }

}
