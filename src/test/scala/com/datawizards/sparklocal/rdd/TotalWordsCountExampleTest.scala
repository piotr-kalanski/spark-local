package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TotalWordsCountExampleTest extends SparkLocalBaseTest {

  val lines = Seq("w1 w2", "w1 w2 w3")

  test("Total words count result") {
    assert(totalWordsCount(RDDAPI(lines)) == 5)
  }

  test("Total words count") {
    assertRDDOperationReturnsSameResult(lines) {
      totalWordsCount
    }
  }

  private def totalWordsCount(data: RDDAPI[String]): Int = {
    data
      .flatMap(line => line.split(" "))
      .map(_  => 1)
      .reduce(_ + _)
  }

}