package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CoalesceTest extends SparkLocalBaseTest {

  test("Coalesce result") {
      val ds = DataSetAPI(Seq(1,2,3))
      assert(ds.coalesce(2) == ds)
  }

  test("Coalesce equal") {
    assertDatasetOperationReturnsSameResultWithSorted(Seq(1,2,3)) {
      ds => ds.coalesce(2)
    }
  }

}