package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UnionTest extends SparkLocalBaseTest {

  test("Union result") {
    assertDatasetOperationResult(
      DataSetAPI(Seq(1,2,3)) union DataSetAPI(Seq(4,5))
    ) {
      Array(1,2,3,4,5)
    }
  }

  test("Union equal - scala union spark") {
    val d2 = DataSetAPI(Seq(1,2,3))
    assertDatasetOperationReturnsSameResult(Seq(4,5)){
      ds => ds union d2
    }
  }

  test("Union equal - spark union scala") {
    val d2 = DataSetAPI(Seq(1,2,3))
    assertDatasetOperationReturnsSameResult(Seq(4,5)){
      ds => d2 union ds
    }
  }

}