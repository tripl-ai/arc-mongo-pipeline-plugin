package ai.tripl.arc.load

import java.net.URI
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

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
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils

class MongoDBLoad extends PipelineStagePlugin {

  val version = ai.tripl.arc.mongodb.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "options" :: "numPartitions" :: "partitionBy" :: "saveMode" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val options = readMap("options", c)
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)  

    (name, description, inputView, numPartitions, saveMode, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(numPartitions), Right(saveMode), Right(partitionBy), Right(invalidKeys)) => 
      
        val stage = MongoDBLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          options=options,
          partitionBy=partitionBy,
          numPartitions=numPartitions,
          saveMode=saveMode,
          params=params
        )

        stage.stageDetail.put("inputView", inputView)  
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, numPartitions, saveMode, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}


case class MongoDBLoadStage(
    plugin: MongoDBLoad,
    name: String, 
    description: Option[String], 
    inputView: String, 
    options: Map[String, String], 
    partitionBy: List[String], 
    numPartitions: Option[Int], 
    saveMode: SaveMode, 
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    MongoDBLoadStage.execute(this)
  }
}

object MongoDBLoadStage {

  def execute(stage: MongoDBLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val df = spark.table(stage.inputView)   

    if (arcContext.isStreaming) {
      throw new Exception("MongoDBLoad does not support streaming mode.") with DetailException {
        override val detail = stage.stageDetail          
      }
    }       

    val listener = ListenerUtils.addStageCompletedListener(stage.stageDetail)

    try {
      stage.partitionBy match {
        case Nil => {
          stage.numPartitions match {
            case Some(n) => df.repartition(n).write.mode(stage.saveMode).format("com.mongodb.spark.sql").options(ReadConfig(stage.options).asOptions).save()
            case None => df.write.mode(stage.saveMode).format("com.mongodb.spark.sql").options(ReadConfig(stage.options).asOptions).save()
          }
        }
        case partitionBy => {
          // create a column array for repartitioning
          val partitionCols = partitionBy.map(col => df(col))
          stage.numPartitions match {
            case Some(n) => df.repartition(n, partitionCols:_*).write.mode(stage.saveMode).format("com.mongodb.spark.sql").options(ReadConfig(stage.options).asOptions).save()
            case None => df.repartition(partitionCols:_*).write.partitionBy(partitionBy:_*).mode(stage.saveMode).format("com.mongodb.spark.sql").options(ReadConfig(stage.options).asOptions).save()
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    spark.sparkContext.removeSparkListener(listener)           

    Option(df)
  }
}