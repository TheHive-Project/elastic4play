name := "elastic4play"

organization := "org.cert-bdf"

organizationName := "CERT-BDF"

organizationHomepage := Some(url("https://thehive-project.org/"))

licenses += "AGPL-V3" -> url("https://www.gnu.org/licenses/agpl-3.0.html")

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  cache,
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.3.0",
  "com.sksamuel.elastic4s" %% "elastic4s-streams" % "2.3.0",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.4" % Test,
  specs2 % Test
)

PlayKeys.externalizeResources := false

bintrayOrganization := Some("cert-bdf")

bintrayRepository := "elastic4play"

publishMavenStyle := true

scalacOptions in ThisBuild ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation", // warning and location for usages of deprecated APIs
  "-feature", // warning and location for usages of features that should be imported explicitly
  "-unchecked", // additional warnings where generated code depends on assumptions
  "-Xlint", // recommended additional warnings
  "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver
  "-Ywarn-value-discard", // Warn when non-Unit expression results are unused
  "-Ywarn-inaccessible",
  "-Ywarn-dead-code"
)
