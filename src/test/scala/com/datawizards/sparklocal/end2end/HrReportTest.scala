package com.datawizards.sparklocal.end2end

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.datastore.{CSVDataStore, JsonDataStore}
import com.datawizards.sparklocal.examples.dataset.Model.{HRReport, Person, WorkExperience}
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import com.datawizards.sparklocal.implicits._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HrReportTest extends SparkLocalBaseTest {

  test("HR report has same result for Spark and Scala") {
    implicit val ordering = new Ordering[HRReport] {
      override def compare(x: HRReport, y: HRReport): Int =
        if(x.year != y.year) x.year.compare(y.year)
        else if(x.title != y.title) x.title.compare(y.title)
        else x.gender.compare(y.gender)
    }

    assertDatasetEquals(
      calculateHRReport(ExecutionEngine.ScalaEager),
      calculateHRReport(ExecutionEngine.Spark)
    )
  }

  def calculateHRReport[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): DataSetAPI[HRReport] = {

    val session = SparkSessionAPI
      .builder(engine)
      .master("local")
      .getOrCreate()

    val people = session.read[Person](JsonDataStore(this.getClass.getResource("/hr_people.json").getPath))
    val workExperience = session.read[WorkExperience](CSVDataStore(this.getClass.getResource("/hr_work_experience.csv").getPath))

    workExperience
      .join(people)(_.personId, _.id)
      .groupByKey(wp => (wp._1.year, wp._1.title, wp._2.gender))
      .mapGroups{case ((year, title, gender), vals) => HRReport(year, title, gender, vals.size)}
  }

}
