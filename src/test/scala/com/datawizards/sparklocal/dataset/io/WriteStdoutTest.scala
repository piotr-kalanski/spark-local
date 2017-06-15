package com.datawizards.sparklocal.dataset.io

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.LargeClass
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.Stdout
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WriteStdoutTest extends SparkLocalBaseTest {

  val data = Seq(
    LargeClass("s1",1,2L,1000000.0,4.0f,5,flag=true,6),
    LargeClass("large string",21,22L,23.0,24.0f,25,flag=false,26)
  )

  test("Write to stdout - Scala") {
    val ds = DataSetAPI(data)

    val expected = """+------------+------+-------+---------+--------+--------+-----+-------+
                     ||strVal      |intVal|longVal|doubleVal|floatVal|shortVal|flag |byteVal|
                     |+------------+------+-------+---------+--------+--------+-----+-------+
                     ||s1          |1     |2      |1000000.0|4.0     |5       |true |6      |
                     ||large string|21    |22     |23.0     |24.0    |25      |false|26     |
                     |+------------+------+-------+---------+--------+--------+-----+-------+
                     |""".stripMargin.replace("\r", "").replace("\n", "")

    val result = ds.write(Stdout()).replace("\r", "").replace("\n", "")

    assertResult(expected) { result }
  }

  test("Write to stdout - Scala and Spark equals") {
    assertDatasetOperationReturnsSameResult(data) {
      ds => ds.show()
    }
    assertDatasetOperationReturnsSameResult(data) {
      ds => ds.show(1)
    }
    assertDatasetOperationReturnsSameResult(data) {
      ds => ds.write(Stdout())
    }
    assertDatasetOperationReturnsSameResult(Seq(1,2,3)) {
      ds => ds.show()
    }
    assertDatasetOperationReturnsSameResult(Seq((1,"a"),(2,"b"))) {
      ds => ds.show()
    }
  }

}