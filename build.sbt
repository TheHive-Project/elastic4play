name := "elastic4play"

organization := "org.thehive-project"

organizationName := "TheHive-Project"

organizationHomepage := Some(url("https://thehive-project.org/"))

licenses += "AGPL-V3" -> url("https://www.gnu.org/licenses/agpl-3.0.html")

lazy val elastic4play = (project in file("."))
  .enablePlugins(PlayScala, PlayAkkaHttp2Support)
// Add Http2 support to be able to ask client certificate
// cf. https://github.com/playframework/playframework/issues/8143

scalaVersion := "2.12.12"

resolvers += "elasticsearch-releases" at "https://artifacts.elastic.co/maven"

val elastic4sVersion = "7.9.1"
libraryDependencies ++= Seq(
  cacheApi,
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-http-streams" % elastic4sVersion,
  "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % elastic4sVersion,
  "com.typesafe.akka"      %% "akka-stream-testkit" % play.core.PlayVersion.akkaVersion % Test,
  "org.scalactic"          %% "scalactic" % "3.1.2",
  "org.bouncycastle"       % "bcprov-jdk15on" % "1.58",
  specs2                   % Test
)

PlayKeys.externalizeResources := false

bintrayOrganization := Some("thehive-project")

bintrayRepository := "maven"

publishMavenStyle := true

scalacOptions in ThisBuild ++= Seq(
  "-encoding",
  "UTF-8",
  "-deprecation",         // warning and location for usages of deprecated APIs
  "-feature",             // warning and location for usages of features that should be imported explicitly
  "-unchecked",           // additional warnings where generated code depends on assumptions
  "-Xlint",               // recommended additional warnings
  "-Ywarn-adapted-args",  // Warn if an argument list is modified to match the receiver
  "-Ywarn-value-discard", // Warn when non-Unit expression results are unused
  "-Ywarn-inaccessible",
  "-Ywarn-dead-code"
)
