package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UnpersistTest extends SparkLocalBaseTest {

  test("Unpersist result") {
      val ds = DataSetAPI(Seq(1,2,3))
      assert(ds.persist().unpersist() == ds)
  }

  test("Unpersist equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.persist().unpersist()
    }
  }

  test("Unpersist(true) equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.persist().unpersist(true)
    }
  }

}