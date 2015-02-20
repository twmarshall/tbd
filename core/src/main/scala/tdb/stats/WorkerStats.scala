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
import org.mashupbots.socko.routes._
import java.io._
import scala.collection.mutable.{Buffer, Map}
import sys.process._

class WorkerTick
    (val numTasks: Int,
     val datastoreMisses: Int)

class WorkerStats extends Actor with ActorLogging {
  val stats = Buffer[WorkerTick]()

  var nextTick = 0

  private def getBytes(path: String): Array[Byte] = {
    val f = new File("webui/" + path)
    val buf = new BufferedInputStream(new FileInputStream("webui/" + path))
    val arr = new Array[Byte](f.length().toInt)
    buf.read(arr)

    arr
  }

  def receive = {
    case "tick" =>
      stats += new WorkerTick(Stats.numTasks, Stats.datastoreMisses)
      nextTick += 1

    case request: HttpRequestEvent =>
      request match {
        case GET(Path("/tasks.png")) =>
          val command = "python webui/chart.py " + stats.map(_.numTasks).mkString(" ")
          println(command)
          val output = command.!!

          request.response.write(getBytes("tasks.png"), "image/png")

        case GET(Path("/")) =>
          var s = "Worker<br>"

          s += """
            <table>
              <tr>
                <td><a href='tasks.png'>tasks</></td>
              </tr>
            </table"""

          val title = "TDB Worker"

          request.response.write(Stats.createPage(title, s), "text/html; charset=UTF-8")
        case _ =>
          request.response.write("This page does not exist.")
      }

    case x =>
      log.warning("Received unhandled message " +
                  x + " from " + sender + " " + x.getClass)
  }
}
