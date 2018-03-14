name := "IDS Project"

version := "2.0"

scalaVersion := "2.11.8"

retrieveManaged := true

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "2.2.1",
    "org.apache.spark" %% "spark-mllib" % "2.2.1",
    "com.typesafe" % "config" % "1.3.1",
    "com.github.scopt" %% "scopt" % "3.7.0"
  )

scalacOptions ++= Seq("-deprecation","-feature")

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

logLevel in assembly := Level.Error