package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IntersectionTest extends SparkLocalBaseTest {

  test("Intersection result - Scala") {
    assertRDDOperationResult(
      RDDAPI(Seq(1,2,3,4)) intersection RDDAPI(Seq(5,4,3))
    ) {
      Array(3,4)
    }
  }

  test("Intersection result - Spark") {
    assertRDDOperationResultWithSorted(
      RDDAPI(sc.parallelize(Seq(1,2,3,4))) intersection RDDAPI(sc.parallelize(Seq(5,4,3)))
    ) {
      Array(3,4)
    }
  }

  test("Intersection result with partitions - Spark") {
    assertRDDOperationResultWithSorted(
      RDDAPI(sc.parallelize(Seq(1,2,3,4))).intersection(RDDAPI(sc.parallelize(Seq(5,4,3))), 2)
    ) {
      Array(3,4)
    }
  }

  test("Intersection result with partitioner - Spark") {
    assertRDDOperationResultWithSorted(
      RDDAPI(sc.parallelize(Seq(1,2,3,4))).intersection(RDDAPI(sc.parallelize(Seq(5,4,3))), new HashPartitioner(2))
    ) {
      Array(3,4)
    }
  }

  test("Intersection equal - scala intersection spark") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => ds intersection r2
    }
  }

  test("Intersection equal - spark intersection scala") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)) {
      ds => r2 intersection ds
    }
  }

  test("Intersection equal with partitions - scala intersection spark") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => ds.intersection(r2,2)
    }
  }

  test("Intersection equal with partitions - spark intersection scala") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => r2.intersection(ds,2)
    }
  }

  test("Intersection equal with partitioner - scala intersection spark") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => ds.intersection(r2,new HashPartitioner(2))
    }
  }

  test("Intersection equal with partitioner - spark intersection scala") {
    val r2 = RDDAPI(Seq(3,4,5))
    assertRDDOperationReturnsSameResultWithSorted(Seq(1,2,3,4)){
      ds => r2.intersection(ds,new HashPartitioner(2))
    }
  }

}
