package com.datawizards.sparklocal

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HeadTest extends SparkLocalBaseTest {

  test("Head result") {
      assert(DataSetAPI(Seq(1,2,3)).head() == 1)
  }

  test("Head(n) result") {
    assert(DataSetAPI(Seq(1,2,3)).head(2) sameElements Array(1,2))
  }


  test("Head equal") {
    assertDatasetOperation(Seq(1,2,3)){
      ds => ds.head()
    }
  }

  test("Head(n) equal") {
    assertDatasetOperation(Seq(1,2,3)){
      ds => ds.head(2)
    }
  }


}