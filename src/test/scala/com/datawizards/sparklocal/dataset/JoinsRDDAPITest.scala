package com.datawizards.sparklocal.dataset

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JoinsRDDAPITest extends SparkLocalBaseTest {

  val left = Seq(
    (1,2000,10),
    (1,2001,11),
    (2,2000,20),
    (2,2001,21),
    (2,2002,22),
    (4,2000,40),
    (4,2001,41)
  )
  val right = Seq(
    (1, "Piotrek"),
    (2, "Pawel"),
    (3, "Michal")
  )
  val leftDataset = DataSetAPI(left)
  val rightDataset = DataSetAPI(right)

  test("join result") {
    assertDatasetOperationResultWithSorted(leftDataset.join(rightDataset)(_._1, _._1)) {
     Array(
       ((1,2000,10), (1,"Piotrek")),
       ((1,2001,11), (1,"Piotrek")),
       ((2,2000,20), (2,"Pawel")),
       ((2,2001,21), (2,"Pawel")),
       ((2,2002,22), (2,"Pawel"))
     )
    }
  }

  test("left join result") {
    assertDatasetOperationResultWithSorted(leftDataset.leftOuterJoin(rightDataset)(_._1, _._1)) {
      Array(
        ((1,2000,10), Some((1,"Piotrek"))),
        ((1,2001,11), Some((1,"Piotrek"))),
        ((2,2000,20), Some((2,"Pawel"))),
        ((2,2001,21), Some((2,"Pawel"))),
        ((2,2002,22), Some((2,"Pawel"))),
        ((4,2000,40), None),
        ((4,2001,41), None)
      )
    }
  }

  test("right join result") {
    assertDatasetOperationResultWithSorted(leftDataset.rightOuterJoin(rightDataset)(_._1, _._1)) {
      Array(
        (Some((1,2000,10)), (1,"Piotrek")),
        (Some((1,2001,11)), (1,"Piotrek")),
        (Some((2,2000,20)), (2,"Pawel")),
        (Some((2,2001,21)), (2,"Pawel")),
        (Some((2,2002,22)), (2,"Pawel")),
        (None, (3,"Michal"))
      )
    }
  }

  test("full outer join result") {
    assertDatasetOperationResultWithSorted(leftDataset.fullOuterJoin(rightDataset)(_._1, _._1)) {
      Array(
        (Some((1,2000,10)), Some((1,"Piotrek"))),
        (Some((1,2001,11)), Some((1,"Piotrek"))),
        (Some((2,2000,20)), Some((2,"Pawel"))),
        (Some((2,2001,21)), Some((2,"Pawel"))),
        (Some((2,2002,22)), Some((2,"Pawel"))),
        (Some((4,2000,40)), None),
        (Some((4,2001,41)), None),
        (None, Some(3,"Michal"))
      )
    }
  }

  test("join - Scala, Spark - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(left){
      ds => ds.join(rightDataset)(_._1, _._1)
    }
  }

  test("left join - Scala, Spark  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(left){
      ds => ds.leftOuterJoin(rightDataset)(_._1, _._1)
    }
  }

  test("right join - Scala, Spark  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(left){
      ds => ds.rightOuterJoin(rightDataset)(_._1, _._1)
    }
  }

  test("full outer join - Scala, Spark  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(left){
      ds => ds.fullOuterJoin(rightDataset)(_._1, _._1)
    }
  }

  test("join - Spark, Scala - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(left){
      ds => rightDataset.join(ds)(_._1, _._1)
    }
  }

  test("left join - Spark, Scala  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(left){
      ds => rightDataset.leftOuterJoin(ds)(_._1, _._1)
    }
  }

  test("right join - Spark, Scala  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(left){
      ds => rightDataset.rightOuterJoin(ds)(_._1, _._1)
    }
  }

  test("full outer join - Spark, Scala  - equals") {
    assertDatasetOperationReturnsSameResultWithSorted(left){
      ds => rightDataset.fullOuterJoin(ds)(_._1, _._1)
    }
  }

}