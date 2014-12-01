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
package tbd.master

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await

import tbd.Constants._
import tbd.worker.Worker

object MasterConnector {
  private var id = 0

  def apply(masterURL: String) = {
      val conf = akkaConf + """
        akka.remote.netty.tcp.port = 2554
      """

    val mutatorAkkaConf = ConfigFactory.parseString(conf)
    val system = ActorSystem(
      "mutatorSystem",
      ConfigFactory.load(mutatorAkkaConf))
    val selection = system.actorSelection(masterURL)
    val future = selection.resolveOne()

    val masterRef = Await.result(future.mapTo[ActorRef], DURATION)    

    new MasterConnector(masterRef, system)
  }

  def apply
      (singleNode: Boolean = true,
       storeType: String = "memory",
       cacheSize: Int = 10000,
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

    id += 1

    val masterRef = system.actorOf(Master.props(), "master")

    if (singleNode) {
      val workerRef = system.actorOf(
        Worker.props(masterRef, storeType, cacheSize), "worker")
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
