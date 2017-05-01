package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WordCountExampleTest extends SparkLocalBaseTest {

  val lines = Seq("w1 w2", "w1 w2 w3", "w1 w4", "w2 w2 w3")

  test("Words count result") {
    assertRDDOperationResultWithSorted(wordsCount(RDDAPI(lines))) {
      Array(("w1", 3),("w2", 4),("w3", 2),("w4",1))
    }
  }

  test("Words count") {
    assertRDDOperationReturnsSameResultWithSorted(lines) {
      wordsCount
    }
  }

  private def wordsCount(data: RDDAPI[String]): RDDAPI[(String, Int)] = {
    data
      .flatMap(line => line.split(" "))
      .map((_,1))
      .reduceByKey(_ + _)
  }

}