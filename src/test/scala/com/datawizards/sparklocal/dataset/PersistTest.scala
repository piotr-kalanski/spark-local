package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.apache.spark.storage.StorageLevel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PersistTest extends SparkLocalBaseTest {

  test("Persist result") {
      val ds = DataSetAPI(Seq(1,2,3))
      assert(ds.persist() == ds)
  }

  test("Persist equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.persist()
    }
  }

  test("Persist(storageLevel) equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.persist(StorageLevel.MEMORY_ONLY)
    }
  }

  test("Persist, unpersist equal") {
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)){
      ds => ds.persist().unpersist()
    }
  }

}