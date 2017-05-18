package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CacheTest extends SparkLocalBaseTest {

  test("Cache result") {
      val ds = DataSetAPI(Seq(1,2,3))
      assert(ds.cache() == ds)
  }

  test("Cache equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.cache()
    }
  }

}