package com.datawizards.sparklocal.examples.dataset

object Model {
  case class Person(id: Int, name: String, gender: String)
  case class WorkExperience(personId: Int, year: Int, title: String)
  case class HRReport(year: Int, title: String, gender: String, count: Int)
}