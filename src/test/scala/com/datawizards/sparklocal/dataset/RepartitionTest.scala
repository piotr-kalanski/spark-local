package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RepartitionTest extends SparkLocalBaseTest {

  test("repartition result") {
      val ds = DataSetAPI(Seq(1,2,3))
      assert(ds.repartition(2) == ds)
  }

  test("repartition equal") {
    assertDatasetOperationReturnsSameResultWithSorted(Seq(1,2,3)) {
      ds => ds.repartition(2)
    }
  }

}