package com.datawizards.sparklocal.dataset.io

import java.io.File

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.datastore.CSVDataStore
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.datawizards.class2csv._
import com.datawizards.sparklocal.dataset.DataSetAPI
import org.apache.spark.sql.SaveMode

@RunWith(classOf[JUnitRunner])
class WriteCSVTest extends SparkLocalBaseTest {

  test("Default CSV file") {
    val file = "target/foo.csv"
    val ds = DataSetAPI(Seq(
      Person("first",10),
      Person("second",11)
    ))

    ds.write(CSVDataStore(file), SaveMode.Overwrite)
    val expected =
      """name,age
        |first,10
        |second,11""".stripMargin

    assertResult(expected) {
      readFileContent(file)
    }
  }

  test("CSV file with ; delimiter") {
    val file = "target/foo_delimiter.csv"
    val ds = DataSetAPI(Seq(
      Person("first",10),
      Person("second",11)
    ))

    ds.write(CSVDataStore(file, delimiter = ';'), SaveMode.Overwrite)
    val expected =
      """name;age
        |first;10
        |second;11""".stripMargin

    assertResult(expected) {
      readFileContent(file)
    }
  }

  test("CSV file without header") {
    val file = "target/foo_without_header.csv"
    val ds = DataSetAPI(Seq(
      Person("first",10),
      Person("second",11)
    ))

    ds.write(CSVDataStore(file, header = false), SaveMode.Overwrite)
    val expected =
      """first,10
        |second,11""".stripMargin

    assertResult(expected) {
      readFileContent(file)
    }
  }

  test("CSV file with quotes") {
    val file = "target/foo_with_quotes.csv"
    val ds = DataSetAPI(Seq(
      Person("first, first",10),
      Person("second, second",11)
    ))

    ds.write(CSVDataStore(file), SaveMode.Overwrite)
    val expected =
      """name,age
        |"first, first",10
        |"second, second",11""".stripMargin

    assertResult(expected) {
      readFileContent(file)
    }
  }

  test("CSV file with custom header and separator") {
    val file = "target/foo_with_custom_header_and_separator.csv"
    val ds = DataSetAPI(Seq(
      Person("p1", 10),
      Person("p2", 20),
      Person("p3", 30),
      Person("p;4", 40)
    ))

    ds.write(CSVDataStore(file, delimiter = ';', columns = Seq("s2","i2")), SaveMode.Overwrite)
    val expected =
      """s2;i2
        |p1;10
        |p2;20
        |p3;30
        |"p;4";40""".stripMargin

    assertResult(expected) {
      readFileContent(file)
    }
  }

  test("Default CSV file - equals") {
    import spark.implicits._

    val file1 = "target/foo1.csv"
    val file2 = "target/foo2.csv"
    val data = Seq(
      Person("first",10),
      Person("second",11)
    )
    val ds1 = DataSetAPI(data)
    val ds2 = DataSetAPI(data.toDS)

    ds1.write(CSVDataStore(file1), SaveMode.Overwrite)
    ds2.write(CSVDataStore(file2), SaveMode.Overwrite)

    assert(readFileContent(file1) == readCSVFileContentFromDirectory(file2))
  }

  test("CSV file with custom header and separator - equals") {
    import spark.implicits._

    val file1 = "target/foo_with_custom_header_and_separator1.csv"
    val file2 = "target/foo_with_custom_header_and_separator2.csv"
    val data = Seq(
      Person("p1", 10),
      Person("p2", 20),
      Person("p3", 30),
      Person("p;4", 40)
    )
    val ds1 = DataSetAPI(data)
    val ds2 = DataSetAPI(data.toDS)

    ds1.write(CSVDataStore(file1, delimiter = ';', columns = Seq("s2","i2")), SaveMode.Overwrite)
    ds2.write(CSVDataStore(file2, delimiter = ';', columns = Seq("s2","i2")), SaveMode.Overwrite)

    assert(readFileContent(file1) == readCSVFileContentFromDirectory(file2))
  }

  private def readFileContent(file: String): String =
    scala.io.Source.fromFile(file).getLines().mkString("\n")

  private def readCSVFileContentFromDirectory(directory: String): String = {
    val dir = new File(directory)
    val csvFile = dir.listFiles().filter(f => f.getName.endsWith(".csv")).head
    readFileContent(csvFile.getPath)
  }

}