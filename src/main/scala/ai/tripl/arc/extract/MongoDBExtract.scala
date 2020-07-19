package ai.tripl.arc.extract

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import com.typesafe.config._

import com.mongodb.spark.config._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.Utils

class MongoDBExtract extends PipelineStagePlugin with JupyterCompleter {

  val version = ai.tripl.arc.mongodb.BuildInfo.version

  val snippet = """{
    |  "type": "MongoDBExtract",
    |  "name": "MongoDBExtract",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "options": {
    |    "uri": "mongodb://username:password@mongo:27017",
    |    "database": "database",
    |    "collection": "collection"
    |  },
    |  "outputView": "customers"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/extract/#mongoextract")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "outputView" :: "authentication" :: "options" :: "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "params" :: Nil
    val invalidKeys = checkValidKeys(c)(expectedKeys)
    val name = getValue[String]("name")
    val params = readMap("params", c)
    val options = readMap("options", c)
    val description = getOptionalValue[String]("description")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")
    val extractColumns = if(c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> getExtractColumns("schemaURI", authentication) _ else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

    (name, description, extractColumns, schemaView, outputView, persist, numPartitions, authentication, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(extractColumns), Right(schemaView), Right(outputView), Right(persist), Right(numPartitions), Right(authentication), Right(partitionBy), Right(invalidKeys)) =>
        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)

        val stage = MongoDBExtractStage(
          plugin=this,
          name=name,
          description=description,
          schema=schema,
          outputView=outputView,
          authentication=authentication,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          options=options
        )

        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, schemaView, outputView, persist, numPartitions, authentication, extractColumns, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class MongoDBExtractStage(
  plugin: MongoDBExtract,
  name: String,
  description: Option[String],
  schema: Either[String, List[ExtractColumn]],
  outputView: String,
  authentication: Option[Authentication],
  options: Map[String, String],
  params: Map[String, String],
  persist: Boolean,
  numPartitions: Option[Int],
  partitionBy: List[String]) extends ExtractPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    MongoDBExtractStage.execute(this)
  }
}

object MongoDBExtractStage {

  def execute(stage: MongoDBExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    if (arcContext.isStreaming) {
      throw new Exception("MongoDBExtract does not support streaming mode.") with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(stage.schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    CloudUtils.setHadoopConfiguration(stage.authentication)

    val df = try {
      spark.read.format("com.mongodb.spark.sql").options(ReadConfig(stage.options).asOptions).load()
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // if incoming dataset has 0 columns then create empty dataset with correct schema
    val emptyDataframeHandlerDF = try {
      if (df.schema.length == 0) {
        stage.stageDetail.put("records", Integer.valueOf(0))
        optionSchema match {
          case Some(s) => spark.createDataFrame(spark.sparkContext.emptyRDD[Row], s)
          case None => throw new Exception(s"MongoDBExtract has produced 0 columns and no schema has been provided to create an empty dataframe.")
        }
      } else {
        df
      }
    } catch {
      case e: Exception => throw new Exception(e.getMessage) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // set column metadata if exists
    val enrichedDF = optionSchema match {
        case Some(schema) => MetadataUtils.setMetadata(emptyDataframeHandlerDF, schema)
        case None => emptyDataframeHandlerDF
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions)
          case None => enrichedDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions, partitionCols:_*)
          case None => enrichedDF.repartition(partitionCols:_*)
        }
      }
    }
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("inputFiles", Integer.valueOf(repartitionedDF.inputFiles.length))
      stage.stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}