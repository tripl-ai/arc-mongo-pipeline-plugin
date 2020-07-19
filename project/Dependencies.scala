import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "3.0.0"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "3.1.0" % "provided"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

  // mongo
  val mongo = "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2"

  // Project
  val etlDeps = Seq(
    scalaTest,

    arc,
    typesafeConfig,

    sparkSql,

    mongo
  )
}