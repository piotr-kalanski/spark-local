package com.datawizards.sparklocal.impl.scala.parallel.session

import com.datawizards.sparklocal.impl.scala.session.BuilderScalaBase

class BuilderScalaParallelImpl extends BuilderScalaBase[SparkSessionAPIScalaParallelImpl] {

  override def getOrCreate(): SparkSessionAPIScalaParallelImpl =
    new SparkSessionAPIScalaParallelImpl

}
