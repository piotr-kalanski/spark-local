package com.datawizards.sparklocal.impl.scala.session

import com.datawizards.sparklocal.session.{Builder, SparkSessionAPI}
import org.apache.spark.SparkConf

trait BuilderScalaBase[Session <: SparkSessionAPI] extends Builder[Session] {

  override def appName(name: String): Builder[Session] =
    this

  override def config(key: String, value: String): Builder[Session] =
    this

  override def config(key: String, value: Long): Builder[Session] =
    this

  override def config(key: String, value: Double): Builder[Session] =
    this

  override def config(key: String, value: Boolean): Builder[Session] =
    this

  override def config(conf: SparkConf): Builder[Session] =
    this

  override def master(master: String): Builder[Session] =
    this

  override def enableHiveSupport(): Builder[Session] =
    this

}
