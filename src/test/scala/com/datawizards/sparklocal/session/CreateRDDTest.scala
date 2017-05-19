package com.datawizards.sparklocal.session

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CreateRDDTest extends SparkLocalBaseTest {

  val data = Array(1,2,3,4,5)

  test("Create RDD - result") {
    assertRDDOperationResult(createRDD(ExecutionEngine.ScalaEager)) {
      data
    }
  }

  test("Create RDD - equals") {
    assertRDDEquals(
      createRDD(ExecutionEngine.ScalaEager),
      createRDD(ExecutionEngine.Spark)
    )
  }

  private def createRDD[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): RDDAPI[Int] =
    SparkSessionAPI
      .builder(engine)
      .master("local")
      .getOrCreate()
      .createRDD(data)

}