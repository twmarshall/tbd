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
package tdb.util

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

import tdb.Constants._
import tdb.messages._

object AkkaUtil {
  def getActorFromURL(url: String, system: ActorSystem): ActorRef = {
    val selection = system.actorSelection(url)
    val future = selection.resolveOne()
    Await.result(future.mapTo[ActorRef], DURATION)
  }

  def sendToTask(taskId: TaskId, taskRef: ActorRef, message: Any, masterRef: ActorRef)
      (onComplete: => Unit)
      (implicit ec: ExecutionContext) {
    val f = ask(taskRef, message)(akka.util.Timeout(1000))

    f.onComplete {
      case Success(f) =>
        onComplete
      case Failure(e) =>
        println("Sending failed to " + taskRef)
        val newTaskRef = Await.result(
          (masterRef ? ResolveMessage(taskId)).mapTo[ActorRef], DURATION)
        println("Retrieved new taskRef = " + newTaskRef)
        sendToTask(taskId, newTaskRef, message, masterRef)(onComplete)
    }
  }
}
