package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PersistTest extends SparkLocalBaseTest {

  test("Persist result") {
      val ds = RDDAPI(Seq(1,2,3))
      assert(ds.persist() == ds)
  }

  test("Persist equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.persist()
    }
  }

  test("Persist, unpersist equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.persist().unpersist()
    }
  }

}