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

import akka.actor.{Actor, ActorLogging}
import org.mashupbots.socko.events.HttpRequestEvent

class MasterStats extends Actor with ActorLogging {

  val imgSrc = "http://thomasdb.cs.cmu.edu/wordpress/wp-content/uploads/2014/08/thomasdb-white.png"

  def receive = {
    case "tick" =>

    case event: HttpRequestEvent => {
      var s = s"""
        <html>
          <head>
            <title>ThomasDB Master</title>
            <style>
              body {
                font-family: calibri;
              }
              table tr td {
                padding: 10px;
              }
            </style>
          </head>
          <body>
           <table>
             <tr>
               <td style=\"background-color: 990000\">
                 <img src=\"$imgSrc\" width=\"50px\">
               </td>
               <td style=\"font-size: 24pt; color: 990000\">
                 ThomasDB Master
               </td>
             </tr>
           </table>"""

      for (address <- Stats.registeredWorkers) {
        s += s"""<a href=\"http://$address\">$address</a>"""
      }

      s += "</body></html>"

      event.response.write(s, "text/html; charset=UTF-8")
    }

    case x =>
      log.warning("Received unhandled message " +
                  x + " from " + sender + " " + x.getClass)
  }
}
