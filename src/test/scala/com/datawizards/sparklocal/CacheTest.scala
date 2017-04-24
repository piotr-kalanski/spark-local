package com.datawizards.sparklocal

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CacheTest extends SparkLocalBaseTest {

  test("Cache result") {
      val ds = DataSetAPI(Seq(1,2,3))
      assert(ds.cache() == ds)
  }

  test("Cache equal") {
    assertDatasetOperation(Seq(1,2,3)){
      ds => ds.cache()
    }
  }

}