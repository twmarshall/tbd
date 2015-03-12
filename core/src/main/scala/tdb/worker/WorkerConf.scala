/**
 * Copyright (C) 2013 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tdb.worker

import org.rogach.scallop._

import tdb.util.Util

class WorkerConf(args: Array[String]) extends ScallopConf(args) {
  version("TDB 0.1 (c) 2014 Carnegie Mellon University")
  banner("Usage: worker.sh [options] master")
  val cacheSize = opt[Int]("cacheSize", 'c', default = Some(10000),
    descr = "The number of elements to keep in the cache, if the " +
    "berkeleydb store is being used")
  val envHomePath = opt[String]("envHomePath",
    default = Some("/tmp/tdb_berkeleydb"), descr = "If using berkeleydb," +
    "the path to where the database should be stored.")
  val ip = opt[String]("ip", 'i', default = Some(Util.getIP()),
    descr = "The ip address to bind to.")
  val port = opt[Int]("port", 'p', default = Some(2553),
    descr = "The port to bind to.")
  val logging = opt[String]("log", 'l', default = Some("DEBUG"),
    descr = "The logging level. Options, by increasing verbosity, are " +
    "OFF, WARNING, INFO, or DEBUG")
  val storeType = opt[String]("store", 's', default = Some("memory"),
    descr = "The type of datastore to use, either memory or berkeleydb")
  val timeout = opt[Int]("timeout", 't', default = Some(100),
    descr = "How long Akka waits on message responses before timing out")
  val webui_port = opt[Int]("webui_port", 'w', default = Some(8889))
  val master = trailArg[String](required = true)
}
