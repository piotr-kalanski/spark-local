package com.datawizards.sparklocal.examples.dataset

import com.datawizards.sparklocal.datastore.{CSVDataStore, JsonDataStore, Stdout}
import com.datawizards.sparklocal.examples.dataset.Model.{HRReport, Person, WorkExperience}
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}

object End2EndExample extends App {

  val start = System.currentTimeMillis()

  val session = SparkSessionAPI
    .builder(ExecutionEngine.ScalaEager)
    .master("local")
    .getOrCreate()

  import session.implicits._

  val people = session.read[Person](JsonDataStore(getClass.getResource("/hr_people.json").getPath))
  val workExperience = session.read[WorkExperience](CSVDataStore(getClass.getResource("/hr_work_experience.csv").getPath))

  workExperience
    .join(people)(_.personId, _.id)
    .groupByKey(wp => (wp._1.year, wp._1.title, wp._2.gender))
    .mapGroups{case ((year, title, gender), vals) => HRReport(year, title, gender, vals.size)}
    .show()

  val total = System.currentTimeMillis() - start
  println("Total time: " + total + " [ms]")

}
