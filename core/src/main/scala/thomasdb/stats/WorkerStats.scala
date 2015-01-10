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
import scala.collection.mutable.Map

class WorkerInfo
    (val numTasks: Int,
     val datastoreMisses: Int)

class WorkerStats extends Actor with ActorLogging {
  val stats = Map[Int, WorkerInfo]()

  var nextTick = 0

  def receive = {
    case "tick" =>
      stats(nextTick) = new WorkerInfo(Stats.numTasks, Stats.datastoreMisses)
      nextTick += 1

    case event: HttpRequestEvent => {
      var s = "Worker<br>"

      s += "<table><tr><td>Time</td><td>Tasks</td><td>Misses</td></tr>"
      for (num <- (0 until nextTick).reverse) {
        val info = stats(num)
        s += "<tr><td>" + num + "</td><td>" + info.numTasks +
             "</td><td>" + info.datastoreMisses + "</tr>"
      }
      s += "</table>"

      event.response.write(s, "text/html; charset=UTF-8")
    }

    case x =>
      log.warning("Received unhandled message " +
                  x + " from " + sender + " " + x.getClass)
  }
}
