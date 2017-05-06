package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CountByValueTest extends SparkLocalBaseTest {

  val data = Seq(1,1,2,2,2,3)

  test("CountByValue result") {
    assert(RDDAPI(data).countByValue() == Map(1 -> 2, 2 -> 3, 3 -> 1))
  }

  test("CountByValue equal") {
    assertRDDOperationReturnsSameResult(data) {
      ds => ds.countByValue()
    }
  }

}
