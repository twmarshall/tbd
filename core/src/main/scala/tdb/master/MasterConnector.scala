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
package tdb.master

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await

import tdb.Constants._
import tdb.Log
import tdb.stats.Stats
import tdb.worker.Worker
import tdb.util.Util
import tdb.worker.WorkerConf

object MasterConnector {
  private var id = 0

  def apply(masterURL: String) = {
    val ip = Util.getIP()
    val conf = akkaConf + s"""
      akka.remote.netty.tcp.hostname = $ip
      akka.remote.netty.tcp.port = 2554
    """

    val mutatorAkkaConf = ConfigFactory.parseString(conf)
    val system = ActorSystem(
      "mutatorSystem",
      ConfigFactory.load(mutatorAkkaConf))
    Log.log = Logging(system, "main")

    val selection = system.actorSelection(masterURL)
    val future = selection.resolveOne()

    val masterRef = Await.result(future.mapTo[ActorRef], DURATION)

    new MasterConnector(masterRef, system)
  }

  def apply
      (singleNode: Boolean = true,
       workerArgs: Array[String] = Array[String](),
       ip: String = "127.0.0.1",
       port: Int = 2552,
       logging: String = "WARNING") = {
    val conf = akkaConf + s"""
      akka.loglevel = $logging

      akka.remote.netty.tcp.hostname = $ip
      akka.remote.netty.tcp.port = $port
    """
    val masterAkkaConf = ConfigFactory.parseString(conf)

    val system = ActorSystem(
      "masterSystem" + id,
      ConfigFactory.load(masterAkkaConf))
    Log.log = Logging(system, "main")

    id += 1
    val masterRef = system.actorOf(Master.props(), "master")

    if (singleNode) {
      val systemURL = "akka.tcp://" + system.name + "@" + ip + ":" + port
      val args = workerArgs ++ Array(systemURL + "/master")
      val workerConf = new WorkerConf(args)

      val workerRef = system.actorOf(
        Worker.props(masterRef, workerConf, systemURL, ""), "worker")

      Await.result(workerRef ? "started", DURATION)
    }

    new MasterConnector(masterRef, system)
  }
}

class MasterConnector
    (val masterRef: ActorRef,
     val system: ActorSystem) {

  def shutdown() {
    system.shutdown()
    system.awaitTermination()
  }
}
