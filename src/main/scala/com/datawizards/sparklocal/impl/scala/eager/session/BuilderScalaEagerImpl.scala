package com.datawizards.sparklocal.impl.scala.eager.session

import com.datawizards.sparklocal.impl.scala.session.BuilderScalaBase

class BuilderScalaEagerImpl extends BuilderScalaBase[SparkSessionAPIScalaEagerImpl] {

  override def getOrCreate(): SparkSessionAPIScalaEagerImpl =
    new SparkSessionAPIScalaEagerImpl

}
