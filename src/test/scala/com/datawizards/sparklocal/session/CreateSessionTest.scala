package com.datawizards.sparklocal.session

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.impl.spark.session.SparkSessionAPISparkImpl
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CreateSessionTest extends SparkLocalBaseTest {

  test("Configuration") {
      val session = SparkSessionAPI
        .builder(ExecutionEngine.Spark)
        .appName("test")
        .master("local")
        .config("long", 1L)
        .config("double", 1.0)
        .config("string", "text")
        .config("bool", value=true)
        .getOrCreate()

    val sparkSession = session match {
      case sparkSession:SparkSessionAPISparkImpl => sparkSession.spark
      case _ => null
    }

    assertResult("1") { sparkSession.conf.get("long") }
    assertResult("1.0") { sparkSession.conf.get("double") }
    assertResult("text") { sparkSession.conf.get("string") }
    assertResult("true") { sparkSession.conf.get("bool") }
    assertResult("test") { sparkSession.conf.get("spark.app.name") }
    assertResult("local") { sparkSession.conf.get("spark.master") }

  }

}
