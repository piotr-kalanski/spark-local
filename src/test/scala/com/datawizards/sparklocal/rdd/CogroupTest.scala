package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.apache.spark.HashPartitioner
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CogroupTest extends SparkLocalBaseTest {

  val data = Seq((1,11),(1,12),(2,22),(3,33),(3,34),(4,44))
  val other1 = Seq((1,"a1"), (1,"a2"), (2,"b"))
  val other2 = Seq((3,30.0), (4,40.0))
  val other3 = Seq((2,-2),(4,-4))

  test("cogroup(other) result") {
      assertRDDOperationResultWithSorted(RDDAPI(data).cogroup(RDDAPI(other1))) {
        Array(
          (1,(Iterable(11,12),Iterable("a1","a2"))),
          (2,(Iterable(22),Iterable("b"))),
          (3,(Iterable(33,34),Iterable.empty)),
          (4,(Iterable(44),Iterable.empty))
        )
      }
  }

  test("cogroup(other1,other2) result") {
    assertRDDOperationResultWithSorted(RDDAPI(data).cogroup(RDDAPI(other1),RDDAPI(other2))) {
      Array(
        (1,(Iterable(11,12),Iterable("a1","a2"),Iterable.empty)),
        (2,(Iterable(22),Iterable("b"),Iterable.empty)),
        (3,(Iterable(33,34),Iterable.empty,Iterable(30.0))),
        (4,(Iterable(44),Iterable.empty,Iterable(40.0)))
      )
    }
  }

  test("cogroup(other1,other2,other3) result") {
    assertRDDOperationResultWithSorted(RDDAPI(data).cogroup(RDDAPI(other1),RDDAPI(other2),RDDAPI(other3))) {
      Array(
        (1,(Iterable(11,12),Iterable("a1","a2"),Iterable.empty,Iterable.empty)),
        (2,(Iterable(22),Iterable("b"),Iterable.empty,Iterable(-2))),
        (3,(Iterable(33,34),Iterable.empty,Iterable(30.0),Iterable.empty)),
        (4,(Iterable(44),Iterable.empty,Iterable(40.0),Iterable(-4)))
      )
    }
  }

  test("cogroup(other) equal - Scala cogroup Spark") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => rdd.cogroup(RDDAPI(other1))
    }
  }

  test("cogroup(other) equal - Spark cogroup Scala") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => RDDAPI(other1).cogroup(rdd)
    }
  }

  test("cogroup(other1,other2) equal - Scala cogroup Spark") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => rdd.cogroup(RDDAPI(other1),RDDAPI(other2))
    }
  }

  test("cogroup(other1,other2) equal - Spark cogroup Scala") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => RDDAPI(other1).cogroup(rdd,RDDAPI(other2))
    }
  }

  test("cogroup(other1,other2,other3) equal - Scala cogroup Spark") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => rdd.cogroup(RDDAPI(other1),RDDAPI(other2),RDDAPI(other3))
    }
  }

  test("cogroup(other1,other2,other3) equal - Spark cogroup Scala") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => RDDAPI(other1).cogroup(rdd,RDDAPI(other2),RDDAPI(other3))
    }
  }

  test("cogroup(other,numPartitions) equal - Scala cogroup Spark") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => rdd.cogroup(RDDAPI(other1), 2)
    }
  }

  test("cogroup(other,numPartitions) equal - Spark cogroup Scala") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => RDDAPI(other1).cogroup(rdd, 2)
    }
  }

  test("cogroup(other1,other2,numPartitions) equal - Scala cogroup Spark") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => rdd.cogroup(RDDAPI(other1),RDDAPI(other2), 2)
    }
  }

  test("cogroup(other1,other2,numPartitions) equal - Spark cogroup Scala") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => RDDAPI(other1).cogroup(rdd,RDDAPI(other2), 2)
    }
  }

  test("cogroup(other1,other2,other3,numPartitions) equal - Scala cogroup Spark") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => rdd.cogroup(RDDAPI(other1),RDDAPI(other2),RDDAPI(other3), 2)
    }
  }

  test("cogroup(other1,other2,other3,numPartitions) equal - Spark cogroup Scala") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => RDDAPI(other1).cogroup(rdd,RDDAPI(other2),RDDAPI(other3), 2)
    }
  }

  test("cogroup(other,partitioner) equal - Scala cogroup Spark") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => rdd.cogroup(RDDAPI(other1), new HashPartitioner(2))
    }
  }

  test("cogroup(other,partitioner) equal - Spark cogroup Scala") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => RDDAPI(other1).cogroup(rdd, new HashPartitioner(2))
    }
  }

  test("cogroup(other1,other2,partitioner) equal - Scala cogroup Spark") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => rdd.cogroup(RDDAPI(other1),RDDAPI(other2), new HashPartitioner(2))
    }
  }

  test("cogroup(other1,other2,partitioner) equal - Spark cogroup Scala") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => RDDAPI(other1).cogroup(rdd,RDDAPI(other2), new HashPartitioner(2))
    }
  }

  test("cogroup(other1,other2,other3,partitioner) equal - Scala cogroup Spark") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => rdd.cogroup(RDDAPI(other1),RDDAPI(other2),RDDAPI(other3), new HashPartitioner(2))
    }
  }

  test("cogroup(other1,other2,other3,partitioner) equal - Spark cogroup Scala") {
    assertRDDOperationReturnsSameResultWithSorted(data){
      rdd => RDDAPI(other1).cogroup(rdd,RDDAPI(other2),RDDAPI(other3), new HashPartitioner(2))
    }
  }

}