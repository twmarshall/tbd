import sbt._
import Keys._

object TBDBuild extends Build {
  val buildOrganization = "edu.cmu.cs"
  val buildVersion      = "0.1-SNAPSHOT"
  val buildScalaVersion = "2.10.0"

  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion,
    fork         := true,
    autoScalaLibrary := true,
    scalacOptions ++= Seq("-feature", "-deprecation")
  )

  val mavenResolver = "Maven Central Server" at "http://central.maven.org/maven2"

  val commonDeps = Seq (
    "berkeleydb"                  % "je"                   % "3.2.76",

    "com.typesafe.akka"          %% "akka-actor"           % "2.3.2",
    "com.typesafe.akka"          %% "akka-remote"          % "2.3.2",
    //"com.typesafe.scala-logging" %% "scala-logging-slf4j"  % "2.0.4",

    "org.rogach"                  % "scallop_2.10"         % "0.9.5",

    "org.scala-lang"             % "scala-pickling_2.10"       % "0.8.0",
    "org.scalatest"              % "scalatest_2.10"            % "2.1.3" % "test",
    "org.scalaz"                 % "scalaz-core_2.10"          % "7.0.6"

  )

  val mkrun = TaskKey[File]("mkrun")
  val mkvisualization = TaskKey[File]("mkvisualization")
  val mksql = TaskKey[File]("mksql")

  lazy val root = Project (
    "root",
    file(".")
  ) aggregate(macros, core, visualization, sql)

  lazy val core = Project (
    "core",
    file("core"),
    settings = buildSettings ++ Seq (
      exportJars:=true,
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

        masterOut
      }
    )
  ) dependsOn(macros)

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

  lazy val sql = Project(
    "sql",
    file("sql"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= (commonDeps 
                          ++ Seq(//"org.apache.spark" % "spark-core_2.10" % "1.1.0" exclude ("org.spark-project.akka",  "akka-actor_2.1.0") exclude ("org.spark-project.akka",  "akka-remote_2.10") exclude ("org.spark-project.akka",  "akka-slf4j_2.10"),
                                  //"org.apache.spark" % "spark-sql_2.10" % "1.1.0",
                                "org.apache.spark" % "spark-assembly_2.10" % "1.1.0",// exclude ("org.spark-project.akka",  "akka-actor_2.10") exclude ("org.spark-project.akka",  "akka-remote_2.10") exclude ("org.spark-project.akka",  "akka-slf4j_2.10"),
                                  //"org.spark-project.akka" % "akka-slf4j_2.11" % "2.3.4-spark",
                                  //"org.spark-project.akka" % "akka-actor_2.11" % "2.3.4-spark",
                                  //"org.spark-project.akka" % "akka-remote_2.11" % "2.3.4-spark",
                                  //"com.typesafe.akka" % "akka-slf4j_2.11" % "2.3.6",
                                  "net.sf.jsqlparser" % "jsqlparser" % "0.8.0")),
      mksql := {
        val classpath = (fullClasspath in Runtime).value.files.absString
        val template = """#!/bin/sh
        java -Xmx8g -Xss256m -classpath "%s" %s $@
        """

        val sql = template.format(classpath, "tbd.sql.RDDRelation")
        val sqlOut = baseDirectory.value / "../bin/sql.sh"
        IO.write(sqlOut, sql)
        sqlOut.setExecutable(true)

        //val test = template.format(classpath, "tbd.sql.Test")
        //val testOut = baseDirectory.value / "../bin/test.sh"
        //IO.write(testOut, test)
        //testOut.setExecutable(true)

        sqlOut
      }
    )
  ) dependsOn(core)

  lazy val macros = Project(
    "macros",
    file("macros"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= (commonDeps
                          ++ Seq("org.scala-lang" % "scala-compiler" % "2.10.4",
                                 "org.scala-lang" % "scala-reflect" % "2.10.4"))
    )
  )
}
