name := "spark-haversine-example"

version := "0.1"

scalaVersion := "2.12.14"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)
