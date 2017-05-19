package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CheckpointTest extends SparkLocalBaseTest {

  test("Checkpoint result") {
      val ds = DataSetAPI(Seq(1,2,3))
      assert(ds.checkpoint() == ds)
  }

  test("Cache equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.checkpoint()
    }
  }

}