import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbt.Keys._

import scala.xml.transform.{RewriteRule, RuleTransformer}

val removeScoverage = new RuleTransformer(
  new RewriteRule {
    private[this] def isGroupScoverage(child: xml.Node): Boolean =
      child.label == "groupId" && child.text == "org.scoverage"

    override def transform(node: xml.Node): Seq[xml.Node] = node match {
      case e: xml.Elem if e.label == "dependency" && e.child.exists(isGroupScoverage) => Nil
      case _ => Seq(node)
    }
  }
)

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
  "-Ywarn-unused-import"
)

val commonSettings = Seq(
  version := "0.1.0",
  scalaVersion := "2.11.8",
  javacOptions += "-Xmx2048M",
  scalacOptions ++= scalacOpts,
  wartremoverErrors in (Compile, compile) ++= Warts.allBut(Wart.NoNeedForMonad, Wart.Any, Wart.AsInstanceOf, Wart.IsInstanceOf, Wart.Nothing, Wart.Throw, Wart.NonUnitStatements),
  wartremoverErrors in (Test, compile) := Seq(),
  ScalariformKeys.preferences := Settings.commonFormattingPreferences,
  pomPostProcess := { (node: xml.Node) => removeScoverage.transform(node).head }
) ++ scalariformSettings


lazy val types = project.in(file("types"))
  .settings(commonSettings)
  .settings(
      name := "flumina-types",
      libraryDependencies ++= Seq(
        "org.typelevel" %% "cats-core" % "0.6.1",
        "org.scodec" %% "scodec-core" % "1.9.0"
      ),
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.8.0")
  )

lazy val akka = project.in(file("akka"))
  .settings(commonSettings)
  .settings(
      name := "flumina-akka",
      parallelExecution in Test := false,
      libraryDependencies ++= Seq(
          "com.typesafe.akka" %% "akka-stream" % "2.4.8",
          //TEST dependencies.. oh my?
          "de.heikoseeberger" %% "akka-log4j" % "1.1.4" % "test",
          "org.apache.logging.log4j" % "log4j-core" % "2.6" % "test",
          "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.6" % "test",
          "org.slf4j" % "slf4j-log4j12" % "1.7.21" % "test",
          "org.slf4j" % "jcl-over-slf4j" % "1.7.12" % "test",
          "com.typesafe.akka" %% "akka-testkit" % "2.4.8" % "test",
          "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.8" % "test",
          "com.ironcorelabs" %% "cats-scalatest" % "1.3.0" % "test",
          "org.apache.kafka" %% "kafka" % "0.10.0.0" % "test", //TODO: wouldn't zookeeper be sufficient?
          "com.spotify" % "docker-client" % "3.5.12" % "test",
          "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % "2.6.0" % "test",
          "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.0" % "test"
      ),
      coverageMinimum := 80,
      coverageFailOnMinimum := false,
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.8.0")
  ).dependsOn(types)