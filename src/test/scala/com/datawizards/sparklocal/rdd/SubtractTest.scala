package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SubtractTest extends SparkLocalBaseTest {

  test("Subtract result - Scala") {
    assertRDDOperationResult(
      RDDAPI(Seq(1,2,3,4)) subtract RDDAPI(Seq(5,4,3))
    ) {
      Array(3,4)
    }
  }

  test("Subtract result - Spark") {
    assertRDDOperationResultWithSorted(
      RDDAPI(sc.parallelize(Seq(1,2,3,4))) subtract RDDAPI(sc.parallelize(Seq(5,4,3)))
    ) {
      Array(3,4)
    }
  }

  test("Subtract result with partitions - Spark") {
    assertRDDOperationResultWithSorted(
      RDDAPI(sc.parallelize(Seq(1,2,3,4))).subtract(RDDAPI(sc.parallelize(Seq(5,4,3))), 2)
    ) {
      Array(3,4)
    }
  }

  test("Subtract result with partitioner - Spark") {
    assertRDDOperationResultWithSorted(
      RDDAPI(sc.parallelize(Seq(1,2,3,4))).subtract(RDDAPI(sc.parallelize(Seq(5,4,3))), new HashPartitioner(2))
    ) {
      Array(3,4)
    }
  }

  test("Subtract equal - scala subtract spark") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => ds subtract r2
    }
  }

  test("Subtract equal - spark subtract scala") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)) {
      ds => r2 subtract ds
    }
  }

  test("Subtract equal with partitions - scala subtract spark") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => ds.subtract(r2,2)
    }
  }

  test("Subtract equal with partitions - spark subtract scala") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => r2.subtract(ds,2)
    }
  }

  test("Subtract equal with partitioner - scala subtract spark") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => ds.subtract(r2,new HashPartitioner(2))
    }
  }

  test("Subtract equal with partitioner - spark subtract scala") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => r2.subtract(ds,new HashPartitioner(2))
    }
  }

}
