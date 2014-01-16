import sbt._
import Keys._

object BuildSettings {
  val buildOrganization = "edu.cmu.cs"
  val buildVersion      = "0.1"
  val buildScalaVersion = "2.10.3"

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

object Resolvers {
  val localResolver = "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
  val typesafeResolver = "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releasesa/"
}

object Dependencies {
  val reefVer = "0.1-SNAPSHOT"

  val reefCommon = "com.microsoft.reef" % "reef-common" % reefVer
  val reefRuntimeLocal = "com.microsoft.reef" % "reef-runtime-local" % reefVer exclude("com.google.protobuf", "protobuf-java")
  val reefRuntimeYarn = "com.microsoft.reef" % "reef-runtime-yarn" % reefVer exclude("com.google.protobuf", "protobuf-java")
  val reefCheckpoint = "com.microsoft.reef" % "reef-checkpoint" % reefVer exclude("com.google.protobuf", "protobuf-java")
  val reefIO = "com.microsoft.reef" % "reef-io" % reefVer exclude("com.google.protobuf", "protobuf-java")

  val akka = "com.typesafe.akka" % "akka-actor_2.10" % "2.2.3"
  val akkaRemote = "com.typesafe.akka" % "akka-remote_2.10" % "2.2.3"

  val postgresql = "org.postgresql" % "postgresql" % "9.2-1002-jdbc4"

  val scalatest = "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

  val logging = "com.typesafe" % "scalalogging-slf4j_2.10" % "1.0.1"
  val slf4j = "org.slf4j" % "slf4j-jdk14" % "1.7.5"
  val log4j = "log4j" % "log4j" % "1.2.17"
}

object IncrementalBuild extends Build {
  import Resolvers._
  import Dependencies._
  import BuildSettings._

  // Sub-project specific dependencies
  val commonDeps = Seq (
    akka,
    //akkaRemote,
    postgresql,
    scalatest,
    logging,
    slf4j,
    log4j
  )

  val reefDeps = Seq(
    reefCommon,
    reefRuntimeLocal,
    reefRuntimeYarn,
    reefCheckpoint,
    reefIO
  )

  def addConflictManager(org: String, name: String, conflictManager: String) =
    ivyModule <<= (ivyModule, streams) map { (module, s) =>
      module.withModule(s.log) { (ivy, desc, _) =>
        import _root_.org.apache.ivy.{core, plugins}
        import core.module.id.ModuleId
        import plugins.matcher.PatternMatcher

        desc.addConflictManager(
          ModuleId.newInstance(org, name),
          ivy.getSettings.getMatcher(PatternMatcher.EXACT),
          ivy.getSettings.getConflictManager("latest-compatible"))
        module
      }
    }

  lazy val root = Project (
    "root", file(".")) aggregate(coreProject, examples)

  lazy val reef = Project(
    "reef",
    file("reef"),
    settings = buildSettings ++ Seq(resolvers += localResolver,
                                    libraryDependencies ++= reefDeps)
  )

  val mkrun = TaskKey[File]("mkrun")
  lazy val coreProject = Project (
    "core",
    file("core"),
    settings = buildSettings ++ Seq (
      resolvers += localResolver,
      addConflictManager("com.google.protobuf", "protobuf-java", "asdf"),
      //dependencyOverrides += "com.google.protobuf" % "protobuf-java" % "2.4.1",
      //unmanagedBase <<= baseDirectory { base => base / "lib" },
      libraryDependencies ++= commonDeps)
  )

  lazy val examples = Project(
    "examples",
    file("examples"),
    settings = buildSettings ++ Seq (
      resolvers += localResolver,
      libraryDependencies ++= commonDeps,
      mkrun <<= (baseDirectory, fullClasspath in Runtime, mainClass in Runtime) map { (base, cp, main) =>
        val template = """#!/bin/sh
        java -classpath "%s":/Users/thomas/incremental/core/target/classes/ \
         -Djava.util.logging.config.class=com.microsoft.reef.util.logging.Config \
         -Dcom.microsoft.reef.runtime.local.folder=/Users/thomas/incremental/examples/target \
        %s
        """
        val mainStr = main getOrElse error("No main class specified")
        val contents = template.format(cp.files.absString, mainStr)
        val out = base / "run.sh"
        IO.write(out, contents)
        out.setExecutable(true)
        out
      }
    )
  ) dependsOn(coreProject)
}
