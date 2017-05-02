package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CacheTest extends SparkLocalBaseTest {

  test("Cache result") {
      val ds = RDDAPI(Seq(1,2,3))
      assert(ds.cache() == ds)
  }

  test("Cache equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.cache()
    }
  }

}