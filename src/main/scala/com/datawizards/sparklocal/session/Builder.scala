package com.datawizards.sparklocal.session

import org.apache.spark.SparkConf

trait Builder[Session <: SparkSessionAPI] {
  /**
    * Sets a name for the application, which will be shown in the Spark web UI.
    * If no application name is set, a randomly generated name will be used.
    */
  def appName(name: String): Builder[Session]

  /**
    * Sets a config option. Options set using this method are automatically propagated to
    * both `SparkConf` and SparkSession's own configuration.
    */
  def config(key: String, value: String): Builder[Session]

  /**
    * Sets a config option. Options set using this method are automatically propagated to
    * both `SparkConf` and SparkSession's own configuration.
    */
  def config(key: String, value: Long): Builder[Session]

  /**
    * Sets a config option. Options set using this method are automatically propagated to
    * both `SparkConf` and SparkSession's own configuration.
    */
  def config(key: String, value: Double): Builder[Session]

  /**
    * Sets a config option. Options set using this method are automatically propagated to
    * both `SparkConf` and SparkSession's own configuration.
    */
  def config(key: String, value: Boolean): Builder[Session]

  /**
    * Sets a list of config options based on the given `SparkConf`.
    */
  def config(conf: SparkConf): Builder[Session]

  /**
    * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
    * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
    */
  def master(master: String): Builder[Session]

  /**
    * Enables Hive support, including connectivity to a persistent Hive metastore, support for
    * Hive serdes, and Hive user-defined functions.
    */
  def enableHiveSupport(): Builder[Session]

  /**
    * Gets an existing [[SparkSessionAPI]] or, if there is no existing one, creates a new
    * one based on the options set in this builder.
    *
    * This method first checks whether there is a valid thread-local SparkSession,
    * and if yes, return that one. It then checks whether there is a valid global
    * default SparkSession, and if yes, return that one. If no valid global default
    * SparkSession exists, the method creates a new SparkSession and assigns the
    * newly created SparkSession as the global default.
    *
    * In case an existing SparkSession is returned, the config options specified in
    * this builder will be applied to the existing SparkSession.
    *
    * @since 2.0.0
    */
  def getOrCreate(): Session
}
