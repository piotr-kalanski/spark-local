package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CountTest extends SparkLocalBaseTest {

  test("Count result") {
      assert(DataSetAPI(Seq(1,2,3)).count() == 3L)
  }

  test("Count equal") {
    assertDatasetOperation(Seq(1,2,3)){
      ds => ds.count()
    }
  }

}