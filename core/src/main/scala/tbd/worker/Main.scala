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
package tbd.worker

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.rogach.scallop._
import scala.concurrent.Await
import scala.concurrent.duration._

import tbd.Constants
import tbd.Constants._
import tbd.messages._

object Main {
  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("TBD 0.1 (c) 2014 Carnegie Mellon University")
      banner("Usage: worker.sh [options] master")
      val cacheSize = opt[Int]("cacheSize", 'c', default = Some(10000),
        descr = "The number of elements to keep in the cache, if the " +
                "berkeleydb store is being used")
      val ip = opt[String]("ip", 'i', default = Some(localhost),
        descr = "The ip address to bind to.")
      val port = opt[Int]("port", 'p', default = Some(2553),
        descr = "The port to bind to.")
      val logging = opt[String]("log", 'l', default = Some("INFO"),
        descr = "The logging level. Options, by increasing verbosity, are " +
        "OFF, WARNING, INFO, or DEBUG")
      val storeType = opt[String]("store", 's', default = Some("memory"),
        descr = "The type of datastore to use, either memory or berkeleydb")
      val timeout = opt[Int]("timeout", 't', default = Some(100),
        descr = "How long Akka waits on message responses before timing out")

      val master = trailArg[String](required = true)
    }

    Constants.DURATION = Conf.timeout.get.get.seconds
    Constants.TIMEOUT = Timeout(Constants.DURATION)

    val ip = Conf.ip.get.get
    val port = Conf.port.get.get
    val master = Conf.master.get.get
    val logging = Conf.logging.get.get

    val conf = akkaConf + s"""
      akka.loglevel = $logging

      akka.remote.netty.tcp.hostname = $ip
      akka.remote.netty.tcp.port = $port
    """

    val workerAkkaConf = ConfigFactory.parseString(conf)

    val system = ActorSystem(
      "workerSystem",
      ConfigFactory.load(workerAkkaConf))

    val selection = system.actorSelection(master)
    val future = selection.resolveOne()
    val masterRef = Await.result(future.mapTo[ActorRef], DURATION)

    system.actorOf(
      Worker.props(
        masterRef,
        Conf.storeType.get.get,
        Conf.cacheSize.get.get),
      "worker")
  }
}
