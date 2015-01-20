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

import akka.actor.{Actor, ActorLogging}
import org.mashupbots.socko.events.HttpRequestEvent

class MasterStats extends Actor with ActorLogging {

  def receive = {
    case "tick" =>

    case event: HttpRequestEvent => {
      var body = s"""
        <table id=\"workerTable\">
          <tr><th>worker id</th><th>webui</th></tr>"""

      for (workerInfo <- Stats.registeredWorkers) {
        val address = workerInfo.webuiAddress
        val workerId = workerInfo.workerId

        body += s"""
          <tr>
            <td>$workerId</td>
            <td><a href=\"http://$address\">$address</a></td>
          </tr>"""
      }
      body += "</table>"

      event.response.write(Stats.createPage(body), "text/html; charset=UTF-8")
    }

    case x =>
      log.warning("Received unhandled message " +
                  x + " from " + sender + " " + x.getClass)
  }
}
