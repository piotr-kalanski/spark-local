package com.datawizards.sparklocal.examples.dataset

import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}

object SimpleExample extends App {
  val start = System.currentTimeMillis()

  val session = SparkSessionAPI
    .builder(ExecutionEngine.ScalaEager)
    .master("local")
    .getOrCreate()

  val data = session.createDataset(Seq((1,"a"), (2,"b")))

  data.show()

  val total = System.currentTimeMillis() - start
  println("Total time: " + total + " [ms]")
}
