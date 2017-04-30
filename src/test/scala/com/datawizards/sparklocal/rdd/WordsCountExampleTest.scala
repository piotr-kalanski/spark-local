package com.datawizards.sparklocal.rdd

import com.datawizards.sparklocal.SparkLocalBaseTest
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WordsCountExampleTest extends SparkLocalBaseTest {

  val lines = Seq("w1 w2", "w1 w2 w3")

  test("Words count result") {
    assert(wordsCount(RDDAPI(lines)) == 5)
  }

  test("Words count") {
    assertRDDOperation(lines) {
      wordsCount
    }
  }

  private def wordsCount(data: RDDAPI[String]): Int = {
    data
      .flatMap(line => line.split(" "))
      .map(_  => 1)
      .reduce(_ + _)
  }

}