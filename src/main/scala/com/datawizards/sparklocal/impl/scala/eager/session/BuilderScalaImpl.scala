package com.datawizards.sparklocal.impl.scala.eager.session

import com.datawizards.sparklocal.session.Builder
import org.apache.spark.SparkConf

class BuilderScalaImpl extends Builder[SparkSessionAPIScalaImpl] {

  override def appName(name: String): Builder[SparkSessionAPIScalaImpl] =
    this

  override def config(key: String, value: String): Builder[SparkSessionAPIScalaImpl] =
    this

  override def config(key: String, value: Long): Builder[SparkSessionAPIScalaImpl] =
    this

  override def config(key: String, value: Double): Builder[SparkSessionAPIScalaImpl] =
    this

  override def config(key: String, value: Boolean): Builder[SparkSessionAPIScalaImpl] =
    this

  override def config(conf: SparkConf): Builder[SparkSessionAPIScalaImpl] =
    this

  override def master(master: String): Builder[SparkSessionAPIScalaImpl] =
    this

  override def enableHiveSupport(): Builder[SparkSessionAPIScalaImpl] =
    this

  override def getOrCreate(): SparkSessionAPIScalaImpl =
    new SparkSessionAPIScalaImpl

}
