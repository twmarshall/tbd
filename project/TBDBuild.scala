import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object TBDBuild extends Build {
  val buildOrganization = "edu.cmu.cs"
  val buildVersion      = "0.1-SNAPSHOT"
  val buildScalaVersion = "2.11.0"

  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion,
    fork         := true,
    autoScalaLibrary := true,
    scalacOptions ++= Seq("-feature", "-deprecation")
  )

  val mavenResolver = "Maven Central Server" at "http://central.maven.org/maven2"

  val reefVer = "0.9"
  val hadoopVer = "2.2.0"
  val reefDeps = Seq (
    "com.microsoft.reef"          % "reef-common"          % reefVer,
    "com.microsoft.reef"          % "reef-runtime-local"   % reefVer,
    "com.microsoft.reef"          % "reef-runtime-yarn"    % reefVer,
    "com.microsoft.reef"          % "reef-checkpoint"      % reefVer,
    "com.microsoft.reef"          % "reef-io"              % reefVer,
    "com.microsoft.reef"          % "reef-annotations"     % reefVer,
    "com.microsoft.reef"          % "reef-poison"          % reefVer
  )

  val commonDeps = Seq (
    "com.sleepycat"               % "je"                   % "5.0.73",

    "com.typesafe.akka"          %% "akka-actor"           % "2.3.2",
    "com.typesafe.akka"          %% "akka-remote"          % "2.3.2",
    "com.typesafe.scala-logging" %% "scala-logging-slf4j"  % "2.0.4",

    "org.rogach"                  % "scallop_2.11"         % "0.9.5",
    "com.github.jsqlparser"       % "jsqlparser"           % "0.8.6",

    "org.scala-lang"             %% "scala-pickling"       % "0.8.0",
    "org.scalatest"              %% "scalatest"            % "2.1.3" % "test",
    "org.scalaz"                 %% "scalaz-core"          % "7.0.6"
  )

  val mkrun = TaskKey[File]("mkrun")
  val mkvisualization = TaskKey[File]("mkvisualization")
  //val mksql = TaskKey[File]("mksql")

  lazy val root = Project (
    "root",
    file(".")
  ) aggregate(macros, core, visualization, reef)

  lazy val core = Project (
    "core",
    file("core"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= commonDeps,
      resolvers += mavenResolver,
      javaOptions += "-Xss128M",
      mkrun := {
        val classpath = (fullClasspath in Runtime).value.files.absString
        val template = """#!/bin/sh
        java -Xmx2g -Xss4m -classpath "%s" %s $@
        """

        val master = template.format(classpath, "tbd.master.Main")
        val masterOut = baseDirectory.value / "../bin/master.sh"
        IO.write(masterOut, master)
        masterOut.setExecutable(true)

        val worker = template.format(classpath, "tbd.worker.Main")
        val workerOut = baseDirectory.value / "../bin/worker.sh"
        IO.write(workerOut, worker)
        workerOut.setExecutable(true)

        val experiment = template.format(classpath, "tbd.examples.list.Experiment")
        val experimentOut = baseDirectory.value / "../bin/experiment.sh"
        IO.write(experimentOut, experiment)
        experimentOut.setExecutable(true)

        val sql = template.format(classpath, "tbd.sql.SQLTest")
        val sqlOut = baseDirectory.value / "../bin/sql.sh"
        IO.write(sqlOut, sql)
        sqlOut.setExecutable(true)

        masterOut
      }
    )
  ) dependsOn(macros)

  lazy val reef = Project (
    "reef",
    file("reef"),
    settings = buildSettings ++ assemblySettings ++ Seq (
      libraryDependencies ++= (reefDeps
                          ++ Seq(
        ("org.apache.hadoop" % "hadoop-common" % "2.2.0").
          exclude("org.sonatype.sisu.inject", "cglib").
          exclude("javax.servlet", "servlet-api").
          exclude("javax.servlet.jsp", "jsp-api"),
        ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.2.0").
          exclude("org.sonatype.sisu.inject", "cglib").
          exclude("javax.servlet", "servlet-api").
          exclude("javax.servlet.jsp", "jsp-api")
      )),
      mergeStrategy in assembly := {
        case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
        case x =>
          val oldStrategy = (mergeStrategy in assembly).value
          oldStrategy(x)
      }
    )
  ) dependsOn(core)

  lazy val visualization = Project(
    "visualization",
    file("visualization"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= (commonDeps
                          ++ Seq("org.scala-lang" % "scala-swing" % "2.11.0-M7")),
      mkvisualization := {
        val classpath = (fullClasspath in Runtime).value.files.absString
        val template = """#!/bin/sh
        java -Xmx8g -Xss256m -classpath "%s" %s $@
        """

        val visualization = template.format(classpath, "tbd.visualization.Main")
        val visualizationOut = baseDirectory.value / "../bin/visualization.sh"
        IO.write(visualizationOut, visualization)
        visualizationOut.setExecutable(true)

        visualizationOut
      }
    )
  ) dependsOn(core)

/*
  lazy val sql = Project(
    "sql",
    file("sql"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= (commonDeps),
      mksql := {
        val classpath = (fullClasspath in Runtime).value.files.absString
        val template = """#!/bin/sh
        java -Xmx8g -Xss256m -classpath "%s" %s $@
        """

        val sql = template.format(classpath, "tbd.sql.SQLTest")
        val sqlOut = baseDirectory.value / "../bin/sql.sh"
        IO.write(sqlOut, sql)
        sqlOut.setExecutable(true)

        sqlOut
        
      }
    )
  ) dependsOn(core)
*/
  lazy val macros = Project(
    "macros",
    file("macros"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= (commonDeps
                          ++ Seq("org.scala-lang" % "scala-compiler" % "2.11.1",
                                 "org.scala-lang" % "scala-reflect" % "2.11.1"))
    )
  )
}
