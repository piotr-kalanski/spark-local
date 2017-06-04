package com.datawizards.sparklocal.impl.spark.session

import com.datawizards.sparklocal.session.Builder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class BuilderSparkImpl(builder: SparkSession.Builder) extends Builder[SparkSessionAPISparkImpl] {

  private def instance(b: SparkSession.Builder) = new BuilderSparkImpl(b)

  override def appName(name: String): Builder[SparkSessionAPISparkImpl] =
    instance(builder.appName(name))

  override def config(key: String, value: String): Builder[SparkSessionAPISparkImpl] =
    instance(builder.config(key, value))

  override def config(key: String, value: Long): Builder[SparkSessionAPISparkImpl] =
    instance(builder.config(key, value))

  override def config(key: String, value: Double): Builder[SparkSessionAPISparkImpl] =
    instance(builder.config(key, value))

  override def config(key: String, value: Boolean): Builder[SparkSessionAPISparkImpl] =
    instance(builder.config(key, value))

  override def config(conf: SparkConf): Builder[SparkSessionAPISparkImpl] =
    instance(builder.config(conf))

  override def master(master: String): Builder[SparkSessionAPISparkImpl] =
    instance(builder.master(master))

  override def enableHiveSupport(): Builder[SparkSessionAPISparkImpl] =
    instance(builder.enableHiveSupport())

  override def getOrCreate(): SparkSessionAPISparkImpl =
    new SparkSessionAPISparkImpl(builder.getOrCreate())
}
