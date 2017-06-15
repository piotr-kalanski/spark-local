package com.datawizards.sparklocal.impl.scala.session

import com.datawizards.sparklocal.broadcast.BroadcastAPI
import com.datawizards.sparklocal.impl.scala.broadcast.BroadcastAPIScalaImpl
import com.datawizards.sparklocal.session.SparkSessionAPI
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

trait SparkSessionAPIScalaBase extends SparkSessionAPI {

  object implicits {
    implicit def enc[T]: Encoder[T] = null
  }

  override def broadcast[T: ClassTag](value: T): BroadcastAPI[T] =
    new BroadcastAPIScalaImpl[T](value)
}
