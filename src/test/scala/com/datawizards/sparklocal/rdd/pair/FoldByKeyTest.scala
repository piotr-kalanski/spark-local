package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FoldByKeyTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("a",2),("b",2),("b",3),("c",1),("c",1),("c",2))

  test("foldByKey result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).foldByKey(0)(_ + _)
    ) {
      Array(("a",3),("b",5),("c",4))
    }
  }

  test("foldByKey(numPartitions) result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).foldByKey(0, 2)(_ + _)
    ) {
      Array(("a",3),("b",5),("c",4))
    }
  }

  test("foldByKey(partitioner) result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).foldByKey(0, new HashPartitioner(2))(_ + _)
    ) {
      Array(("a",3),("b",5),("c",4))
    }
  }

  test("foldByKey equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.foldByKey(0)(_ + _)
    }
  }

  test("foldByKey(numPartitions) equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.foldByKey(0, 1)(_ + _)
    }
  }

  test("foldByKey(partitioner) equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.foldByKey(0, new HashPartitioner(2))(_ + _)
    }
  }

}