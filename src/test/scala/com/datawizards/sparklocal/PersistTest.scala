package com.datawizards.sparklocal

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PersistTest extends SparkLocalBaseTest {

  test("Persist result") {
      val ds = DataSetAPI(Seq(1,2,3))
      assert(ds.persist() == ds)
  }

  test("Persist equal") {
    assertDatasetOperation(Seq(1,2,3)){
      ds => ds.persist()
    }
  }

}