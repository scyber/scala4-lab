


// Project setup
val rootPackage = "ru.example"
val subRootPackage = s"$rootPackage.scala4-lab"
val projectV = "0.0.1-SNAPSHOT"
val scalaV = "2.12.12"



lazy val settings = Seq(
  organization := s"$subRootPackage",
  version := projectV,
  scalaVersion := scalaV,
  test in assembly := {},

  scalaSource in Compile := baseDirectory.value / "src/main/scala",
  scalaSource in Test := baseDirectory.value / "scc/test/scala",

  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),

  initialCommands in console := "import ru.example.scala4-lab._",

  parallelExecution :=false,

)

lazy val rootProject = project.in(file("."))
  .settings(
    name := "scala4-lab",
    organization := rootPackage,
    version := projectV
  ).aggregate(lab1)
lazy val lab1 = project.settings(
  name := "lab1",
  libraryDependencies ++=CommonDependencies ++ testDependencies
)
lazy val CommonDependencies = Seq(
  //Add any need for project
  Dependency.spark,
  Dependency.scalalogginig,



)
lazy val testDependencies = Seq(
  Dependency.scalaTest
)

// code formatter, executed on goal:compile by default
//scalariformPreferences := scalariformPreferences.value
//  .setPreference(AlignSingleLineCaseStatements, true)
//  .setPreference(DoubleIndentConstructorArguments, true)
//  .setPreference(DanglingCloseParenthesis, Preserve)