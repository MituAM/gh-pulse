// gh-pulse — one Scala core, two execution engines.
//
// `core` holds the domain model and every business rule as pure, engine-agnostic Scala.
// `spark` and `flink` are thin adapters that hand the same rules to a batch or a streaming
// runtime, which is what lets the two engines be tested for agreement (see BatchStreamParitySpec).

lazy val sparkVersion = "3.5.1"
lazy val flinkVersion = "1.18.1"
lazy val circeVersion = "0.14.6"
lazy val scalaTestVersion = "3.2.18"
lazy val log4jVersion = "2.20.0"

ThisBuild / organization := "io.github.ghpulse"
ThisBuild / version := "0.1.0"
// Spark 3.5 and Flink 1.18 both publish their Scala APIs for 2.12 only, which pins the build.
ThisBuild / scalaVersion := "2.12.18"

// Spark and Flink reach into JDK internals that JEP 403 sealed off, so tests have to re-open
// them. The flags are rejected outright by a Java 8 launcher, hence the version guard.
lazy val jdkModuleOpens: Seq[String] = {
  val modules = Seq(
    "java.base/java.lang",
    "java.base/java.lang.invoke",
    "java.base/java.lang.reflect",
    "java.base/java.io",
    "java.base/java.net",
    "java.base/java.nio",
    "java.base/java.util",
    "java.base/java.util.concurrent",
    "java.base/java.util.concurrent.atomic",
    "java.base/sun.nio.ch",
    "java.base/sun.nio.cs",
    "java.base/sun.security.action",
    "java.base/sun.util.calendar"
  )
  if (scala.util.Properties.isJavaAtLeast("11")) modules.map(m => s"--add-opens=$m=ALL-UNNAMED")
  else Seq.empty
}

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-encoding",
    "utf-8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-unused:imports,privates,locals"
  ),
  Test / fork := true,
  Test / javaOptions ++= jdkModuleOpens ++ Seq("-Xmx2g"),
  // -oD prints the duration of every test, which makes a slow Spark/Flink suite obvious in CI.
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.scalatestplus" %% "scalacheck-1-17" % "3.2.18.0" % Test
  )
)

// Spark and Flink are `Provided` so the assembled jars stay small and never fight the versions
// already installed on a cluster. sbt puts `Provided` on the test classpath automatically but
// not on the `run` classpath, so `run` is rewired to use the full one.
lazy val runWithProvidedDeps = Seq(
  Compile / run := Defaults
    .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
    .evaluated,
  Compile / runMain := Defaults
    .runMainTask(Compile / fullClasspath, Compile / run / runner)
    .evaluated,
  Compile / run / fork := true,
  Compile / run / javaOptions ++= jdkModuleOpens,
  // Run from the repository root so `--input data/sample/events.jsonl` means the same thing
  // whether it is typed at the root or inside a module.
  Compile / run / baseDirectory := (ThisBuild / baseDirectory).value,
  // A stable jar name; the deployment commands in the README refer to it.
  assembly / assemblyJarName := s"${name.value}.jar",
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _ @_*) => MergeStrategy.concat
    case PathList("META-INF", _ @_*) => MergeStrategy.discard
    case "reference.conf" => MergeStrategy.concat
    case "module-info.class" => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)

lazy val core = (project in file("modules/core"))
  .settings(commonSettings)
  .settings(
    name := "gh-pulse-core",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion
    )
  )

lazy val spark = (project in file("modules/spark"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings, runWithProvidedDeps)
  .settings(
    name := "gh-pulse-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
    ),
    // Spark's own log4j2 defaults are set to INFO, which buries both the test report and the
    // demo's output. These configs are only ever selected explicitly, so a cluster deployment
    // keeps using the cluster's own logging setup.
    Test / javaOptions += "-Dlog4j.configurationFile=log4j2-test.properties",
    Compile / run / javaOptions += "-Dlog4j.configurationFile=conf/log4j2.properties"
  )

lazy val flink = (project in file("modules/flink"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings, runWithProvidedDeps)
  .settings(
    name := "gh-pulse-flink",
    // Flink 1.18 deprecated its Scala DataStream API wholesale (FLIP-265); it is still the most
    // idiomatic way to drive Flink from Scala, so the deprecation noise is muted here only.
    scalacOptions --= Seq("-deprecation", "-Xlint"),
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % flinkVersion % Provided,
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % Provided,
      // `flink-clients` carries the local mini cluster the tests run their jobs on, and sbt puts
      // `Provided` on the test classpath, so no separate test harness dependency is needed.
      "org.apache.flink" % "flink-clients" % flinkVersion % Provided,
      "org.apache.flink" % "flink-connector-files" % flinkVersion % Provided,
      // Flink logs through slf4j but ships no binding; without one every test run opens with a
      // "Failed to load class StaticLoggerBinder" warning and hides any error Flink does report.
      // log4j2 picks up log4j2-test.properties from the test classpath on its own.
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion % Test,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion % Test
    )
  )

lazy val root = (project in file("."))
  .aggregate(core, spark, flink)
  .settings(
    name := "gh-pulse",
    publish / skip := true
  )

// The committed sample covers a single hour, so the demos use ten-minute windows to give the
// timeline and the streaming leaderboard something to move between.
addCommandAlias(
  "demoSpark",
  "spark/runMain ghpulse.spark.SparkPipeline --input data/sample/events.jsonl --window 10 --top 10"
)
addCommandAlias(
  "demoFlink",
  "flink/runMain ghpulse.flink.FlinkPipeline --input data/sample/events.jsonl --window 10 --top 5"
)
// Exactly what CI runs, so a green `sbt verify` locally means a green build.
addCommandAlias("verify", ";scalafmtCheckAll;scalafmtSbtCheck;test")
