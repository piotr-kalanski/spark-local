package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CollectTest extends SparkLocalBaseTest {

  test("Collect result") {
    assert(DataSetAPI(Seq(1,2,3)).collect() sameElements Array(1,2,3))
  }

  test("Collect() equal") {
    def collect:(DataSetAPI[Int] => Array[Int]) = ds => ds.collect()

    assertDatasetOperationReturnsSameResultWithEqual(Seq(1,2,3), collect) {
      case(r1,r2) => r1 sameElements r2
    }
  }

  test("CollectAsList() equal") {
    def collect:(DataSetAPI[Int] => java.util.List[Int]) = ds => ds.collectAsList()

    assertDatasetOperationReturnsSameResultWithEqual(Seq(1,2,3), collect) {
      case(r1,r2) => r1.equals(r2)
    }
  }

}