package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DiffTest extends SparkLocalBaseTest {

  test("Diff result - Scala") {
    assertDatasetOperationResult(
      DataSetAPI(Seq(1,2,3)) diff DataSetAPI(Seq(2,3))
    ) {
      Array(1)
    }
  }

  test("Union equal - scala diff spark") {
    val ds2 = DataSetAPI(Seq(1,2,3))
    assertDatasetOperationReturnsSameResult(Seq(2,3)){
      ds => ds diff ds2
    }
  }

  test("Union equal - spark diff scala") {
    val ds2 = DataSetAPI(Seq(1,2,3))
    assertDatasetOperationReturnsSameResult(Seq(2,3)){
      ds => ds2 diff ds
    }
  }

}