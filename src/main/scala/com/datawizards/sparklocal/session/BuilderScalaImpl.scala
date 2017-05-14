package com.datawizards.sparklocal.session
import org.apache.spark.SparkConf

class BuilderScalaImpl extends Builder {

  override def appName(name: String): Builder =
    this

  override def config(key: String, value: String): Builder =
    this

  override def config(key: String, value: Long): Builder =
    this

  override def config(key: String, value: Double): Builder =
    this

  override def config(key: String, value: Boolean): Builder =
    this

  override def config(conf: SparkConf): Builder =
    this

  override def master(master: String): Builder =
    this

  override def enableHiveSupport(): Builder =
    this

  override def getOrCreate(): SparkSessionAPI =
    SparkSessionAPIScalaImpl

}
