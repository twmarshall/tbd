import sbt._
import Keys._

object BuildSettings {
  val buildOrganization = "edu.cmu.cs"
  val buildVersion      = "0.1-SNAPSHOT"
  val buildScalaVersion = "2.11.0"

  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := buildOrganization,
    version      := buildVersion,
    scalaVersion := buildScalaVersion,
    shellPrompt  := ShellPrompt.buildShellPrompt,
    fork         := true
  )
}

// Shell prompt which show the current project,
// git branch and build version
object ShellPrompt {
  object devnull extends ProcessLogger {
    def info (s: => String) {}
    def error (s: => String) { }
    def buffer[T] (f: => T): T = f
  }
  def currBranch = (
    ("git status -sb" lines_! devnull headOption)
      getOrElse "-" stripPrefix "## "
  )

  val buildShellPrompt = {
    (state: State) => {
      val currProject = Project.extract (state).currentProject.id
      "%s:%s:%s> ".format (
        currProject, currBranch, BuildSettings.buildVersion
      )
    }
  }
}

object TBDBuild extends Build {
  import BuildSettings._

  val commonDeps = Seq (
    "com.typesafe.akka"          %% "akka-actor"           % "2.3.2",
    "com.typesafe.scala-logging" %% "scala-logging-slf4j"  % "2.0.4",
    "org.scala-lang.modules"      % "scala-xml_2.11.0-RC1" % "1.0.0",
    "org.scalatest"              %% "scalatest"            % "2.1.3" % "test"
  )

  lazy val root = Project (
    "root", file(".")) aggregate(core, examples)

  lazy val core = Project (
    "core",
    file("core"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= commonDeps)
  )

  val mkrun = TaskKey[File]("mkrun")
  lazy val examples = Project(
    "examples",
    file("examples"),
    settings = buildSettings ++ Seq (
      libraryDependencies ++= commonDeps,
      mkrun <<= (baseDirectory, fullClasspath in Runtime) map { (base, cp) =>
        val template = """#!/bin/sh
        java -Xmx2g -Xss4m -classpath "%s" %s $@
        """
        val contents = template.format(cp.files.absString, "tbd.examples.wordcount.Experiment")
        val out = base / "run.sh"
        IO.write(out, contents)
        out.setExecutable(true)
        out
      }
    )
  ) dependsOn(core)
}
