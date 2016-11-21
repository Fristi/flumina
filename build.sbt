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
  "-Ywarn-unused-import",
  "-Ypartial-unification"
)

val commonSettings = Seq(
  version := "0.1.0",
  organization := "net.vectos",
  scalaVersion := "2.12.0",
  javacOptions += "-Xmx2048M",
  scalacOptions ++= scalacOpts,
  wartremoverErrors in (Compile, compile) ++= Warts.allBut(Wart.NoNeedForMonad, Wart.Any, Wart.AsInstanceOf, Wart.IsInstanceOf, Wart.Nothing, Wart.Throw, Wart.NonUnitStatements),
  wartremoverErrors in (Test, compile) := Seq(),
  ScalariformKeys.preferences := Settings.commonFormattingPreferences,
  pomPostProcess := { (node: xml.Node) => removeScoverage.transform(node).head },
  resolvers += Resolver.sonatypeRepo("releases")
) ++ scalariformSettings


lazy val core = project.in(file("core"))
  .settings(commonSettings)
  .settings(
      name := "flumina-core",
      libraryDependencies ++= Seq(
        "org.typelevel" %% "cats-free" % "0.8.1",
        "org.scodec" %% "scodec-core" % "1.10.3",
        "org.scodec" %% "scodec-bits" % "1.1.2",
        "org.xerial.snappy" % "snappy-java" % "1.1.2.6"
      ),
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")
  )

lazy val akka = project.in(file("akka"))
  .settings(commonSettings)
  .settings(
      name := "flumina-akka",
      parallelExecution in Test := false,
      libraryDependencies ++= Seq(
          "com.typesafe.akka" %% "akka-actor" % "2.4.13",
          //TEST dependencies.. oh my?
          "de.heikoseeberger" %% "akka-log4j" % "1.2.0" % "test",
          "org.apache.logging.log4j" % "log4j-core" % "2.6" % "test",
          "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.6" % "test",
          "org.slf4j" % "slf4j-log4j12" % "1.7.21" % "test",
          "org.slf4j" % "jcl-over-slf4j" % "1.7.12" % "test",
          "com.typesafe.akka" %% "akka-testkit" % "2.4.13" % "test",
          "com.ironcorelabs" %% "cats-scalatest" % "2.1.1" % "test",
          "com.spotify" % "docker-client" % "3.5.12" % "test",
          "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % "2.6.0" % "test",
          "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.0" % "test"
      ),
      coverageMinimum := 80,
      coverageFailOnMinimum := false,
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")
  ).dependsOn(core)