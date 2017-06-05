package com.datawizards.sparklocal.impl.scala.`lazy`.session

import com.datawizards.sparklocal.impl.scala.session.BuilderScalaBase

class BuilderScalaLazyImpl extends BuilderScalaBase[SparkSessionAPIScalaLazyImpl] {

  override def getOrCreate(): SparkSessionAPIScalaLazyImpl =
    new SparkSessionAPIScalaLazyImpl

}
