package com.datawizards.sparklocal.dataset.io

import com.datawizards.esclient.repository.ElasticsearchRepositoryImpl
import com.datawizards.sparklocal.SparkLocalBaseTest
import com.datawizards.sparklocal.TestModel.Person
import com.datawizards.sparklocal.datastore.ElasticsearchSimpleIndexDataStore
import com.datawizards.sparklocal.implicits._
import com.datawizards.sparklocal.session.{ExecutionEngine, SparkSessionAPI}
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ElasticsearchIntegrationTest extends SparkLocalBaseTest {
  private val repository = new ElasticsearchRepositoryImpl("http://localhost:9200")
  private val indexName = "testindex"
  private val typeName = "testtype"
  private val dataStore = ElasticsearchSimpleIndexDataStore("localhost", indexName, typeName)
  private val mapping =
    """{
      |   "mappings" : {
      |      "Person" : {
      |         "properties" : {
      |            "name" : {"type" : "string"},
      |            "age" : {"type" : "integer"}
      |         }
      |      }
      |   }
      |}""".stripMargin

  test("Run tests") {
    // Run integration tests if Elasticsearch cluster is running
    if(repository.status()) {
      writeTest(ExecutionEngine.ScalaEager)
      writeTest(ExecutionEngine.Spark)
    }
    else {
      println("Elasticsearch instance not running!")
    }
  }

  protected def writeTest[Session <: SparkSessionAPI](engine: ExecutionEngine[Session]): Unit = {
    val session = SparkSessionAPI.builder(engine).master("local").getOrCreate()
    val data = Seq(
      Person("p1", 10),
      Person("p2", 20)
    )
    val ds = session.createDataset(data)
    ds.write.es(dataStore, SaveMode.Overwrite)
    Thread.sleep(1000)
    var result = repository.search[Person](indexName)
    assertResult(2)(result.total)
    assert(data.sorted == result.hits.toSeq.sorted)
    intercept[Exception] {
      ds.write.es(dataStore, SaveMode.ErrorIfExists)
    }
    ds.write.es(dataStore, SaveMode.Append)
    Thread.sleep(1000)
    result = repository.search[Person](indexName)
    assertResult(4)(result.total)
    ds.write.es(dataStore, SaveMode.Ignore)
    Thread.sleep(1000)
    result = repository.search[Person](indexName)
    assertResult(4)(result.total)
  }

}