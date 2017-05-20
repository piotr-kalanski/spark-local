package com.datawizards.sparklocal.dataset.grouped

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CogroupTest extends SparkLocalBaseTest {

  val data = Seq((1,1),(1,2),(2,3),(3,33))
  val other = Seq((1,2),(1,3),(2,3))

  test("cogroup(other) result - Scala") {
      assertDatasetOperationResultWithSorted(
        DataSetAPI(data)
          .groupByKey(_._1)
          .mapValues(x => x._2)
          .cogroup(DataSetAPI(other).groupByKey(_._1).mapValues(x => x._2)) {
            case (k,xs,ys) =>
              val ysSum = ys.sum
              xs.map(x => (k, x*ysSum))
          }
      ) {
        Array(
          (1, 5),
          (1, 10),
          (2, 9),
          (3, 0)
        )
      }
  }

  test("cogroup(other) result - Spark") {
    import spark.implicits._

    assertDatasetOperationResultWithSorted(
      DataSetAPI(data.toDS())
        .groupByKey(_._1)
        .mapValues(x => x._2)
        .cogroup(DataSetAPI(other.toDS()).groupByKey(_._1).mapValues(x => x._2)) {
          case (k,xs,ys) =>
            val ysSum = ys.sum
            xs.map(x => (k, x*ysSum))
        }
    ) {
      Array(
        (1, 5),
        (1, 10),
        (2, 9),
        (3, 0)
      )
    }
  }

  test("cogroup(other) equal - Scala cogroup Spark") {
    assertDatasetOperationReturnsSameResultWithSorted(data){
      ds => ds.groupByKey(_._1)
        .mapValues(x => x._2)
        .cogroup(DataSetAPI(other).groupByKey(_._1).mapValues(x => x._2)) {
          case (k,xs,ys) =>
            val ysSum = ys.sum
            xs.map(x => (k, x*ysSum))
        }
    }
  }

  test("cogroup(other) equal - Spark cogroup Scala") {
    assertDatasetOperationReturnsSameResultWithSorted(data){
      ds => DataSetAPI(other).groupByKey(_._1)
        .mapValues(x => x._2)
        .cogroup(ds.groupByKey(_._1).mapValues(x => x._2)) {
          case (k,xs,ys) =>
            val ysSum = ys.sum
            xs.map(x => (k, x*ysSum))
        }
    }
  }

}