package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CheckpointTest extends SparkLocalBaseTest {

  test("Checkpoint result") {
      val ds = RDDAPI(Seq(1,2,3))
      assert(ds.checkpoint() == ds)
  }

  test("Cache equal") {
    assertRDDOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.checkpoint()
    }
  }

}