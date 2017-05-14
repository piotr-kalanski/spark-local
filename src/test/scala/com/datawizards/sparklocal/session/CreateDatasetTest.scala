package com.datawizards.sparklocal.session

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.session.ExecutionEngine.ExecutionEngine
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CreateDatasetTest extends SparkLocalBaseTest {

  val data = Array(1,2,3,4,5)

  test("Create DataSet - result") {
    assertDatasetOperationResult(createDataset(ExecutionEngine.ScalaEager)) {
      data
    }
  }

  test("Create DataSet - equals") {
    assertDatasetEquals(
      createDataset(ExecutionEngine.ScalaEager),
      createDataset(ExecutionEngine.Spark)
    )
  }

  private def createDataset(engine: ExecutionEngine): DataSetAPI[Int] =
    SparkSessionAPI
      .builder(engine)
      .master("local")
      .getOrCreate()
      .createDataset(data)

}