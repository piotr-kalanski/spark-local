package com.datawizards.sparklocal.dataset.io

import java.io.File

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.JsonDataStore
import com.datawizards.sparklocal.implicits._
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteJsonTest extends SparkLocalBaseTest {

  test("Write Json file") {
    val file = "target/foo.json"
    val ds = DataSetAPI(Seq(
      Person("first",10),
      Person("second",11)
    ))

    ds.write(JsonDataStore(file), SaveMode.Overwrite)
    val expected =
      """{"name":"first","age":10}
        |{"name":"second","age":11}""".stripMargin

    assertResult(expected.replace("\r", "").replace("\n", "")) {
      readJsonFileContentFromDirectory(file).replace("\r", "").replace("\n", "")
    }
  }

  test("Write JSON file - equals") {
    import spark.implicits._

    val file1 = "target/foo1.json"
    val file2 = "target/foo2.json"
    val data = Seq(
      Person("first",10),
      Person("second",11)
    )
    val ds1 = DataSetAPI(data)
    val ds2 = DataSetAPI(data.toDS)

    ds1.write(JsonDataStore(file1), SaveMode.Overwrite)
    ds2.write(JsonDataStore(file2), SaveMode.Overwrite)

    assert(readJsonFileContentFromDirectory(file1) == readJsonFileContentFromDirectory(file2))
  }

  private def readFileContent(file: String): String =
    scala.io.Source.fromFile(file).getLines().mkString("\n")

  private def readJsonFileContentFromDirectory(directory: String): String = {
    val dir = new File(directory)
    val csvFile = dir.listFiles().filter(f => f.getName.endsWith(".json")).head
    readFileContent(csvFile.getPath)
  }

}