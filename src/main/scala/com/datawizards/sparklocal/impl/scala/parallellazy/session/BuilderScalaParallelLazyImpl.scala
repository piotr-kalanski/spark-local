package com.datawizards.sparklocal.impl.scala.parallellazy.session

import com.datawizards.sparklocal.impl.scala.session.BuilderScalaBase

class BuilderScalaParallelLazyImpl extends BuilderScalaBase[SparkSessionAPIScalaParallelLazyImpl] {

  override def getOrCreate(): SparkSessionAPIScalaParallelLazyImpl =
    new SparkSessionAPIScalaParallelLazyImpl

}
