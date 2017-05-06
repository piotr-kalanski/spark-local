package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CacheTest extends SparkLocalBaseTest {

  test("Cache result") {
      val rdd = RDDAPI(Seq(1,2,3))
      assert(rdd.cache() == rdd)
  }

  test("Cache equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      rdd => rdd.cache()
    }
  }

}