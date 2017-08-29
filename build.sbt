name := "SparkAnalyse"

version := "1.0"

scalaVersion := "2.11.8"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.16.0"
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.4.0"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "{latestVersion}"
val elastic4sVersion = "6.0.0-M3"
libraryDependencies ++= Seq(
                             // for the http client
                             "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion)
libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }