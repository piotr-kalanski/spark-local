package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GroupByKeyTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("a",2),("b",2),("b",3),("c",1),("c",1),("c",2))

  test("groupByKey result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).groupByKey()
    ) {
      Array(("a",Iterable(1,2)),("b",Iterable(2,3)),("c",Iterable(1,1,2)))
    }
  }

  test("groupByKey(numPartitions) result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).groupByKey(2)
    ) {
      Array(("a",Iterable(1,2)),("b",Iterable(2,3)),("c",Iterable(1,1,2)))
    }
  }

  test("groupByKey(partitioner) result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).groupByKey(new HashPartitioner(2))
    ) {
      Array(("a",Iterable(1,2)),("b",Iterable(2,3)),("c",Iterable(1,1,2)))
    }
  }

  test("groupByKey equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.groupByKey()
    }
  }

  test("groupByKey(numPartitions) equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.groupByKey(2)
    }
  }

  test("groupByKey(partitioner) equal") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.groupByKey(new HashPartitioner(2))
    }
  }

}