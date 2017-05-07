package com.datawizards.sparklocal.dataset.grouped

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.dataset.DataSetAPI
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KeysTest extends SparkLocalBaseTest {

  test("Keys result") {
    assertDatasetOperationResultWithSorted(
      DataSetAPI(Seq(("a",1),("b",2))).groupByKey(_._1).keys
    ) {
      Array("a","b")
    }
  }

  test("Keys equal") {
    assertDatasetOperationReturnsSameResult(Seq(("a",1),("b",2))){
      ds => ds.groupByKey(_._1).keys
    }
  }


}