artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "tfidfRecommender.jar" }

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "ScalableAndCloudProgrammingProject" ,
  )


    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"
