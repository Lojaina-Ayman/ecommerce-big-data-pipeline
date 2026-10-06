ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version      := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "ecommerce-pipeline",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.2" % Provided,
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
  )
