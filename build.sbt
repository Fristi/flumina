import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbt.Keys._
import sbt.addCompilerPlugin

import scala.xml.transform.{RewriteRule, RuleTransformer}

val akkaVersion = "2.5.2"
val catsVersion = "0.9.0"
val monixVersion = "2.2.4"

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

lazy val doNotPublishArtifact = Seq(
  publishArtifact := false,
  publishArtifact in (Compile, packageDoc) := false,
  publishArtifact in (Compile, packageSrc) := false,
  publishArtifact in (Compile, packageBin) := false
)

val commonSettings = Seq(
  version := "0.1.0",
  organization := "net.vectos",
  scalaVersion := "2.12.1",
  scalacOptions ++= Seq(
    "-language:higherKinds",
    "-feature",
    "-deprecation",
    "-Yno-adapted-args",
    "-Xlint",
    "-Xfatal-warnings",
    "-unchecked"
  ),
  scalacOptions in compile ++= Seq(
    "-Yno-imports",
    "-Ywarn-numeric-widen"
  ),
  pomPostProcess := { (node: xml.Node) => removeScoverage.transform(node).head },
  resolvers += Resolver.sonatypeRepo("releases"),
  resolvers += "Confluent Repo" at "http://packages.confluent.io/maven",
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4")
)

val codeQualitySettings = Seq(
  wartremoverErrors in (Compile, compile) ++= Warts.allBut(Wart.DefaultArguments, Wart.StringPlusAny, Wart.Recursion, Wart.ArrayEquals, Wart.ImplicitParameter, Wart.Equals, Wart.Any, Wart.AsInstanceOf, Wart.IsInstanceOf, Wart.Nothing, Wart.Throw, Wart.NonUnitStatements),
  wartremoverErrors in (Test, compile) := Seq(),
  ScalariformKeys.preferences := Settings.commonFormattingPreferences
) ++ scalariformSettings

val codeProjectSettings = commonSettings ++ codeQualitySettings

lazy val core = project.in(file("core"))
  .settings(codeProjectSettings)
  .settings(
      name := "flumina-core",
      libraryDependencies ++= Seq(
        "org.typelevel" %% "cats-free" % catsVersion,
        "org.scodec" %% "scodec-core" % "1.10.3",
        "org.scodec" %% "scodec-bits" % "1.1.2",
        "org.xerial.snappy" % "snappy-java" % "1.1.2.6"
      )
  )


lazy val akka = project.in(file("akka"))
  .settings(codeProjectSettings)
  .settings(
      name := "flumina-akka",
      libraryDependencies ++= Seq(
          "com.github.melrief" %% "pureconfig" % "0.6.0",
          "com.typesafe.akka" %% "akka-actor" % akkaVersion
      )
  ).dependsOn(core)


lazy val avro4s = project.in(file("avro4s"))
  .settings(codeProjectSettings)
  .settings(
    name := "flumina-avro4s",
    libraryDependencies ++= Seq(
      "io.confluent" % "kafka-schema-registry-client" % "3.2.1",
      "com.sksamuel.avro4s" %% "avro4s-core" % "1.6.4"
    )
  ).dependsOn(core)


lazy val monix = project.in(file("monix"))
  .settings(codeProjectSettings)
  .settings(
    name := "flumina-monix",
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
      "io.monix" %% "monix" % monixVersion,
      "io.monix" %% "monix-cats" % monixVersion
    )
).dependsOn(akka)

lazy val docs = project
  .in(file("docs"))
  .settings(doNotPublishArtifact)
  .settings(commonSettings)
  .dependsOn(core, akka, monix, avro4s)
  .enablePlugins(MicrositesPlugin)
  .settings(name := "flumina-docs")
  .settings(
    micrositeName             := "Flumina",
    micrositeDescription      := "Scala Kafka client",
    micrositeAuthor           := "Mark de Jong",
    micrositeGithubOwner      := "vectos",
    micrositeGithubRepo       := "flumina",
    micrositeBaseUrl          := "/flumina",
    micrositeDocumentationUrl := "/flumina/docs/",
    micrositeHighlightTheme   := "color-brewer"
  )

lazy val tests = project.in(file("tests"))
  .settings(commonSettings)
  .settings(doNotPublishArtifact)
  .settings(
    name := "flumina-tests",
    parallelExecution in Test := false,
    coverageMinimum := 80,
    coverageFailOnMinimum := false,
    wartremoverErrors in (Test, compile) := Seq(),
    libraryDependencies ++= Seq(
      "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.5" % Test,
      "org.typelevel" %% "cats-laws" % catsVersion % Test,
      "org.typelevel" %% "cats-kernel-laws" % catsVersion % Test,
      "com.ironcorelabs" %% "cats-scalatest" % "2.2.0" % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.spotify" % "docker-client" % "3.5.13" % Test,
      "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % "2.6.7" % Test,
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7" % Test
    )
)
.dependsOn(monix, avro4s)


addCommandAlias("validate", "; clean; tests/test; docs/makeMicrosite")
