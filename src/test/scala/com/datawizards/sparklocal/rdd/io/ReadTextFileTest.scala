package com.datawizards.sparklocal.rdd.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadTextFileTest extends SparkLocalBaseTest {

  test("textFile equals") {
      assertRDDEquals(
        SparkSessionAPI.builder(ExecutionEngine.ScalaEager).master("local").getOrCreate().textFile(getClass.getResource("/data.txt").getPath),
        SparkSessionAPI.builder(ExecutionEngine.Spark).master("local").getOrCreate().textFile(getClass.getResource("/data.txt").getPath)
      )
  }

}
