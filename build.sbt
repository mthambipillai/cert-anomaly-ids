name := "IDS Project"

version := "2.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "2.2.1",
    "org.apache.spark" %% "spark-mllib" % "2.2.1"
  )

scalacOptions ++= Seq("-deprecation","-feature")