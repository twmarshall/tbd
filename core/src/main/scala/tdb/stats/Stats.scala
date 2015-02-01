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
package tdb.stats

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import org.mashupbots.socko.events.HttpRequestEvent
import org.mashupbots.socko.routes._
import org.mashupbots.socko.infrastructure.Logger
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}
import scala.collection.mutable.Buffer
import scala.concurrent.duration._

object Stats {
  var registeredWorkers = Buffer[WorkerInfo]()

  // Worker
  var numTasks = 0

  var datastoreMisses = 0

  val imgSrc = "http://thomasdb.cs.cmu.edu/wordpress/wp-content/uploads/2014/08/thomasdb-white.png"

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

  def createPage(title: String, body: String): String =
    s"""
      <!DOCTYPE html>
      <html>
        <head>
          <title>$title</title>
          <style>
            body {
              font-family: calibri;
              color: #333333;
            }

            body a {
              text-decoration: none;
            }

            body a:hover {
              text-decoration: underline;
            }

            #mainTable tr td {
              padding: 10px;
            }

            #workerTable {
              font-family: \"Trebuchet MS\", Arial, Helvetica, sans-serif;
              width: 100%;
              border-collapse: collapse;
            }

            #workerTable td, #workerTable th {
              font-size: 1em;
              border: 1px solid #333333;
              padding: 3px 7px 2px 7px;
            }

            #workerTable th {
              font-size: 1.1em;
              text-align: left;
              padding-top: 5px;
              padding-bottom: 4px;
              background-color: #333333;
              color: #ffffff;
            }
          </style>
          <script src=\"Chart.js\"></script>
          <script type=\"text/javascript\">
            // Get the context of the canvas element we want to select
//var ctx = document.getElementById(\"myChart\").getContext(\"2d\");
//var myNewChart = new Chart(ctx).PolarArea(data);
          </script>
        </head>
        <body>
          <table id=\"mainTable\">
            <tr>
              <td style=\"background-color: #990000;\">
                <img src=\"$imgSrc\" width=\"50px\">
              </td>
              <td style=\"font-size: 24pt;\">$title</td>
            </tr>
            <tr>
              <td colspan=2>$body
<canvas id=\"myChart\" width=\"400\" height=\"400\"></canvas>
              </td>
            </tr>
          </table>
        </body>
      </html>"""
}
