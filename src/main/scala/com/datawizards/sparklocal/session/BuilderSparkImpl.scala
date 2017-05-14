package com.datawizards.sparklocal.session

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class BuilderSparkImpl(builder: SparkSession.Builder) extends Builder {

  private def instance(b: SparkSession.Builder) = new BuilderSparkImpl(b)

  override def appName(name: String): Builder =
    instance(builder.appName(name))

  override def config(key: String, value: String): Builder =
    instance(builder.config(key, value))

  override def config(key: String, value: Long): Builder =
    instance(builder.config(key, value))

  override def config(key: String, value: Double): Builder =
    instance(builder.config(key, value))

  override def config(key: String, value: Boolean): Builder =
    instance(builder.config(key, value))

  override def config(conf: SparkConf): Builder =
    instance(builder.config(conf))

  override def master(master: String): Builder =
    instance(builder.master(master))

  override def enableHiveSupport(): Builder =
    instance(builder.enableHiveSupport())

  override def getOrCreate(): SparkSessionAPI =
    new SparkSessionAPISparkImpl(builder.getOrCreate())
}
