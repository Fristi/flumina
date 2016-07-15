import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbt.Keys._

val scalacOpts = List(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:experimental.macros",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Yrangepos",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused-import"
)

val commonSettings = Seq(
  version := "0.1.0",
  scalaVersion := "2.11.8",
  javacOptions += "-Xmx2048M",
  scalacOptions ++= scalacOpts,
  wartremoverErrors in (Compile, compile) ++= Warts.allBut(Wart.NoNeedForMonad, Wart.Any, Wart.AsInstanceOf, Wart.IsInstanceOf, Wart.Nothing, Wart.Throw),
  wartremoverErrors in (Test, compile) := Seq(),
  ScalariformKeys.preferences := Settings.commonFormattingPreferences
) ++ scalariformSettings


lazy val types = project.in(file("types"))
      .settings(commonSettings)
      .settings(
          name := "scala-kafka-types",
          libraryDependencies ++= Seq("org.scodec" %% "scodec-core" % "1.9.0")
      )

lazy val akka = project.in(file("akka"))
      .settings(commonSettings)
      .settings(
          name := "scala-kafka-akka",
          libraryDependencies ++= Seq(
              "com.lihaoyi" %% "pprint" % "0.4.1",
              "org.typelevel" % "scala-reflect" % "2.11.7",
              "com.typesafe.akka" %% "akka-stream" % "2.4.8",
              "de.heikoseeberger" %% "akka-log4j" % "1.1.4",
              "org.apache.logging.log4j" % "log4j-core" % "2.6",
              "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.6",
              "org.slf4j" % "slf4j-log4j12" % "1.7.21" % "test",
              "org.slf4j" % "jcl-over-slf4j" % "1.7.12" % "test",
              "net.manub" %% "scalatest-embedded-kafka" % "0.6.1" % "test",
              "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.8" % "test"
          )
      ).dependsOn(types)



