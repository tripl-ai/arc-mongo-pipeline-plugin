package ai.tripl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api.API._
import ai.tripl.arc.api._
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.util.ControlUtils._
import ai.tripl.arc.util._

class MongoDBExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val outputView = "actual"
  val user = "root"
  val pass = "example"
  val host = "mongo"
  val port = "27017"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop
  }

  test("MongoDBExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "MongoDBExtract",
          "name": "load customers",
          "environments": [
            "production",
            "test"
          ],
          "options": {
            "uri": "mongodb://${user}:${pass}@${host}:${port}",
            "database": "local",
            "collection": "startup_log"
          }
          "outputView": "customer"
        }
      ]
    }"""


    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    // assert graph created
    pipelineEither match {
      case Left(err) => {
        println(err)
        assert(false)
      }
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext)
        assert(df.get.count != 0)
      }
    }
  }

  test("MongoDBExtract: Missing collection") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "MongoDBExtract",
          "name": "load customers",
          "environments": [
            "production",
            "test"
          ],
          "options": {
            "uri": "mongodb://${user}:${pass}@${host}:${port}",
            "database": "local",
            "collection": "none"
          }
          "outputView": "customer"
        }
      ]
    }"""


    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    // assert graph created
    pipelineEither match {
      case Left(err) => {
        println(err)
        assert(false)
      }
      case Right((pipeline, _)) => {
        val thrown0 = intercept[Exception with DetailException] {
          ARC.run(pipeline)(spark, logger, arcContext)
        }
        assert(thrown0.getMessage === "MongoDBExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
      }
    }
  }

}
