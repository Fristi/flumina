import sbt.Keys._

lazy val types = project.in(file("types"))
      .settings(
          name := "scala-kafka-types",
          version := "1.0",
          scalaVersion := "2.11.8",
          libraryDependencies ++= Seq("org.scodec" %% "scodec-core" % "1.9.0")
      )

lazy val akka = project.in(file("akka"))
      .settings(
          name := "scala-kafka-akka",
          version := "1.0",
          scalaVersion := "2.11.8",
          libraryDependencies ++= Seq(
              "com.typesafe.akka" %% "akka-stream" % "2.4.7",
              "de.heikoseeberger" %% "akka-log4j" % "1.1.4",
              "org.apache.logging.log4j" % "log4j-core" % "2.6",
              "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.6",
              "org.slf4j" % "slf4j-log4j12" % "1.7.21" % "test",
              "org.slf4j" % "jcl-over-slf4j" % "1.7.12" % "test",
              "net.manub" %% "scalatest-embedded-kafka" % "0.6.1" % "test",
              "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.7" % "test"
          )
      ).dependsOn(types)