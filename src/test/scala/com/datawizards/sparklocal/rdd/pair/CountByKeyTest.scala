package com.datawizards.sparklocal.rdd.pair

import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.rdd.RDDAPI
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CountByKeyTest extends SparkLocalBaseTest {

  val data = Seq(("a",1),("a",2),("b",2),("b",3),("c",1),("c",1),("c",2))

  test("countByKey result") {
    assert(RDDAPI(data).countByKey() == Map(("a",2),("b",2),("c",3)))
  }

  test("countByKey equal") {
    assertRDDOperationReturnsSameResult(data){
      ds => ds.countByKey()
    }
  }

}