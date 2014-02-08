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

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.mutable.Set
import scala.concurrent.Await
import scala.concurrent.duration._

import tbd.TBD
import tbd.ddg.DDG
import tbd.messages._
import tbd.mod.ModId

object Worker {
  def props[T](id: Int, datastoreRef: ActorRef): Props =
    Props(classOf[Worker[T]], id, datastoreRef)
}

class Worker[T](id: Int, datastoreRef: ActorRef)
  extends Actor with ActorLogging {
  log.info("Worker " + id + " launched")
  private var task: Task = null
  private val ddg = new DDG()

  implicit val timeout = Timeout(5 seconds)

  def receive = {
    case ModUpdatedMessage(modId) => {
      ddg.modUpdated(modId)
      sender ! "okay"
    }
    case RunTaskMessage(aTask: Task) => {
      task = aTask
      val tbd = new TBD(ddg, datastoreRef, self, context.system, true)
      val output = task.func(tbd)
      sender ! output.mod
    }

    case PropagateMessage => {
      log.debug("Worker" + id + " actor asked to perform change propagation.")
      log.debug("Contents of the DDG:\n" + ddg)

      while (!ddg.updated.isEmpty) {
        val readNode = ddg.updated.dequeue
        log.debug("Updating " + readNode.toString(""))
        ddg.removeSubtree(readNode)
        val future = datastoreRef ? GetMessage("mods", readNode.modId.value)
        val newValue = Await.result(future, timeout.duration)
        readNode.reader(newValue)
      }

      sender ! "done"
    }

    case x => log.warning("Worker" + id + " actor received unhandled message " +
			  x + " from " + sender)
  }
}
