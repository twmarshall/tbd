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
package thomasdb.stats

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.mashupbots.socko.events.HttpRequestEvent
import org.mashupbots.socko.routes._
import org.mashupbots.socko.infrastructure.Logger
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}
import scala.collection.mutable.Buffer
import scala.concurrent.duration._

object Stats {
  var registeredWorkers = Buffer[String]()

  // Worker
  var numTasks = 0

  var datastoreMisses = 0

  def launch(system: ActorSystem, mode: String, host: String, port: Int) {
    val statsActor =
      if (mode == "master")
        system.actorOf(Props(classOf[MasterStats]))
      else
        system.actorOf(Props(classOf[WorkerStats]))

    val routes = Routes({
      case GET(request) => {
        statsActor ! request
      }
    })

    val webServer =
      new WebServer(
        WebServerConfig(hostname = host, port = port),
        routes,
        system)

    webServer.start()

    import system.dispatcher

    val cancellable =
      system.scheduler.schedule(0.milliseconds,
        1000.milliseconds,
        statsActor,
        "tick")
  }
}
