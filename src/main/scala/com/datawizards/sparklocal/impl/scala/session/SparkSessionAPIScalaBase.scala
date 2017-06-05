package com.datawizards.sparklocal.impl.scala.session

import com.datawizards.sparklocal.session.SparkSessionAPI
import org.apache.spark.sql.Encoder

trait SparkSessionAPIScalaBase extends SparkSessionAPI {

  object implicits {
    implicit def enc[T]: Encoder[T] = null
  }

}
