package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GroupByTest extends SparkLocalBaseTest {

  val data:Seq[(String, Int)] = Seq(("a",1),("a",2),("b",2),("b",3),("c",1),("c",1),("c",2))

  test("groupBy result") {
    assertRDDOperationResultWithSorted(
      RDDAPI(data).groupBy(_._1)
    ) {
      Array(("a",Iterable(("a", 1),("a", 2))),("b",Iterable(("b",2),("b",3))),("c",Iterable(("c",1),("c",1),("c",2))))
    }
  }

  test("groupBy(numPartitions) result") {
    def key(p:(String,Int)): String = p._1
    val k = key _
    assertRDDOperationResultWithSorted(
      RDDAPI(data).groupBy[String](k, 2)
    ) {
      Array(("a",Iterable(("a", 1),("a", 2))),("b",Iterable(("b",2),("b",3))),("c",Iterable(("c",1),("c",1),("c",2))))
    }
  }

  test("groupBy(partitioner) result") {
    def key(p:(String,Int)): String = p._1
    val k = key _
    assertRDDOperationResultWithSorted(
      RDDAPI(data).groupBy[String](k, new HashPartitioner(2))
    ) {
      Array(("a",Iterable(("a", 1),("a", 2))),("b",Iterable(("b",2),("b",3))),("c",Iterable(("c",1),("c",1),("c",2))))
    }
  }

  test("groupBy equal") {
    def key(p:(String,Int)): String = p._1
    val k = key _
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.groupBy(k)
    }
  }

  test("groupBy(numPartitions) equal") {
    def key(p:(String,Int)): String = p._1
    val k = key _
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.groupBy(k, 2)
    }
  }

  test("groupBy(partitioner) equal") {
    def key(p:(String,Int)): String = p._1
    val k = key _
    assertRDDOperationReturnsSameResultWithSorted(data){
      ds => ds.groupBy(k, new HashPartitioner(2))
    }
  }

}