package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CoalesceTest extends SparkLocalBaseTest {

  test("Coalesce result") {
      val rdd = RDDAPI(Seq(1,2,3))
      assert(rdd.coalesce(2) == rdd)
  }

  test("Coalesce equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      rdd => rdd.coalesce(2)
    }
  }

}