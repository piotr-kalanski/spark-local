package com.datawizards.sparklocal.dataset.io

import com.datawizards.dmg.dialects.{DecoratorDialect, Dialect}

object ModelDialects {
  val CSV: Dialect = new DummyDialect
  val Parquet: Dialect = new DummyDialect
  val Avro: Dialect = new DummyDialect
  val JSON: Dialect = new DummyDialect

  private class DummyDialect extends DecoratorDialect(null) {
    override protected def decorate(dataModel: String): String = null
  }
}
