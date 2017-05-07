package com.datawizards.sparklocal.examples.dataset

import com.datawizards.sparklocal.dataset.DataSetAPI
import com.datawizards.sparklocal.examples.dataset.Model._
import org.apache.spark.sql.SparkSession

object ExampleHRReport {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val people = SampleData.people
    val peopleDs = people.toDS()
    val experience = SampleData.experience
    val experienceDs = experience.toDS()

    assertEquals(
      calculateReport(DataSetAPI(people), DataSetAPI(experience)),
      calculateReport(DataSetAPI(peopleDs), DataSetAPI(experienceDs))
    )

  }

  def assertEquals(r1:DataSetAPI[HRReport], r2:DataSetAPI[HRReport]): Unit = {
    def collectAndSort(ds:DataSetAPI[HRReport]): Array[HRReport] = {
      ds.collect().sortBy(r => (r.year, r.title, r.gender))
    }
    val s1 = collectAndSort(r1)
    val s2 = collectAndSort(r2)

    def printReport(report: Array[HRReport]): Unit = {
      for(line <- report)
        println(s"${line.year}\t${line.title.padTo(20," ").mkString("")}\t${line.gender}\t${line.count}")
    }

    println("=============== (Scala): ===============")
    printReport(s1)
    println("=============== (Spark): ===============")
    printReport(s2)

    assert(s1 sameElements s2)
  }

  def calculateReport(people: DataSetAPI[Person], workExperience: DataSetAPI[WorkExperience]): DataSetAPI[HRReport] = {
    workExperience
      .join(people)(_.personId, _.id)
      .groupByKey(wp => (wp._1.year, wp._1.title, wp._2.gender))
      .mapGroups{case ((year, title, gender), vals) => HRReport(year, title, gender, vals.size)}
  }

}