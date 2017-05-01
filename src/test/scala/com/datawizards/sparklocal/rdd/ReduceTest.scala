package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReduceTest extends SparkLocalBaseTest {

  test("Reduce result") {
      assert(RDDAPI(Seq(1,2,3)).reduce(_ + _) == 6)
  }

  test("Reduce equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.reduce(_ + _)
    }
  }

}