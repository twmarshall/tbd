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

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.event.Logging
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._

import tdb.{Constants, Log}
import tdb.Constants._
import tdb.messages._
import tdb.stats.Stats

object Main {
  def main(args: Array[String]) {
    val conf = new WorkerConf(args)

    Constants.DURATION = conf.timeout().seconds
    Constants.TIMEOUT = Timeout(Constants.DURATION)

    val ip = conf.ip()
    val port = conf.port()
    val master = conf.master()
    val logging = conf.logging()
    val webui_port = conf.webui_port()

    val workerAkkaconf = {
      val conf = akkaConf + s"""
        akka.loglevel = $logging

        akka.remote.netty.tcp.hostname = $ip
        akka.remote.netty.tcp.port = $port
      """
      ConfigFactory.parseString(conf)
    }

    val system = ActorSystem(
      "workerSystem",
      ConfigFactory.load(workerAkkaconf))

    val selection = system.actorSelection(master)
    val future = selection.resolveOne()
    val masterRef = Await.result(future.mapTo[ActorRef], DURATION)

    val systemURL = "akka.tcp://" + system.name + "@" + ip + ":" + port
    system.actorOf(
      Worker.props(
        masterRef,
        conf,
        systemURL,
        ip + ":" + webui_port),
      "worker")

    Log.log = Logging(system, "worker")

    Stats.launch(system, "worker", ip, webui_port)
  }
}
