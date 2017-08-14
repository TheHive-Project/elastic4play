name := "elastic4play"

organization := "org.cert-bdf"

organizationName := "CERT-BDF"

organizationHomepage := Some(url("https://thehive-project.org/"))

licenses += "AGPL-V3" -> url("https://www.gnu.org/licenses/agpl-3.0.html")

lazy val elastic4play = (project in file("."))
  .enablePlugins(PlayScala)

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  cacheApi,
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.4.1",
  "com.sksamuel.elastic4s" %% "elastic4s-streams" % "2.4.1",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.4" % Test,
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

SbtScalariform.defaultScalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignParameters, false)
//  .setPreference(FirstParameterOnNewline, Force)
  .setPreference(AlignArguments, true)
//  .setPreference(FirstArgumentOnNewline, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 60)
  .setPreference(CompactControlReadability, true)
  .setPreference(CompactStringConcatenation, false)
  .setPreference(DoubleIndentClassDeclaration, true)
//  .setPreference(DoubleIndentMethodDeclaration, true)
  .setPreference(FormatXml, true)
  .setPreference(IndentLocalDefs, false)
  .setPreference(IndentPackageBlocks, false)
  .setPreference(IndentSpaces, 2)
  .setPreference(IndentWithTabs, false)
  .setPreference(MultilineScaladocCommentsStartOnFirstLine, false)
//  .setPreference(NewlineAtEndOfFile, true)
  .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, false)
  .setPreference(PreserveSpaceBeforeArguments, false)
//  .setPreference(PreserveDanglingCloseParenthesis, false)
  .setPreference(DanglingCloseParenthesis, Prevent)
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(SpaceBeforeColon, false)
//  .setPreference(SpaceBeforeContextColon, false)
  .setPreference(SpaceInsideBrackets, false)
  .setPreference(SpaceInsideParentheses, false)
  .setPreference(SpacesWithinPatternBinders, true)
  .setPreference(SpacesAroundMultiImports, true)
