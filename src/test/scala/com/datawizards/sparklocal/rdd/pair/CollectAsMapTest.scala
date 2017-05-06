package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CollectAsMapTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("b",2),("c",3))

  test("CollectAsMap result") {
    assert(RDDAPI(data).collectAsMap() == Map("a"->1,"b"->2,"c"->3))
  }

  test("CollectAsMap equal") {
    assertRDDOperationReturnsSameResult(data) {
      ds => ds.collectAsMap()
    }
  }

}