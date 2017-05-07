package com.datawizards.sparklocal.examples.dataset

import com.datawizards.sparklocal.examples.dataset.Model._

object SampleData {
  val people = Seq(
    Person(1, "p1", "M"),
    Person(2, "p2", "F"),
    Person(3, "p3", "M"),
    Person(4, "p4", "F"),
    Person(5, "p5", "F"),
    Person(6, "p6", "M"),
    Person(7, "p6", "M"),
    Person(8, "p6", "M")
  )

  val experience = Seq(
    WorkExperience(1, 2000, "Java Developer"),
    WorkExperience(1, 2001, "Java Developer"),
    WorkExperience(2, 2000, "Business Analyst"),
    WorkExperience(2, 2001, "Business Analyst"),
    WorkExperience(3, 2000, "Project Manager"),
    WorkExperience(4, 2000, "Scala Developer"),
    WorkExperience(4, 2001, "Big Data Developer"),
    WorkExperience(5, 2000, "QA"),
    WorkExperience(5, 2001, "QA"),
    WorkExperience(6, 2001, "Scrum Master"),
    WorkExperience(7, 2000, "Scala Developer"),
    WorkExperience(7, 2001, "Scala Developer"),
    WorkExperience(8, 2001, "Java Developer")
  )
}
