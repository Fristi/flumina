logLevel := Level.Warn

addSbtPlugin("com.updateimpact" % "updateimpact-sbt-plugin" % "2.1.1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")

addSbtPlugin("org.wartremover" % "sbt-wartremover" % "1.2.1")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0-M14")

addSbtPlugin("pl.project13.scala" %  "sbt-jmh" % "0.2.17")