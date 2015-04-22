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
package tdb

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}


import tdb.Constants._
import tdb.messages._

class Resolver(masterRef: ActorRef) {
  val tasks = mutable.Map[TaskId, ActorRef]()

  def resolve(taskId: TaskId): ActorRef = {
    if (!tasks.contains(taskId)) {
      val taskRef = Await.result(
        (masterRef ? ResolveMessage(taskId)).mapTo[ActorRef], DURATION)
      tasks(taskId) = taskRef
    }

    tasks(taskId)
  }

  def sendToTask(taskId: TaskId, message: Any, round: Int = 1)
      (onComplete: => Unit)
      (implicit ec: ExecutionContext) {
    val taskRef = resolve(taskId)
    val f = ask(taskRef, message)(Timeout(1000 * round, java.util.concurrent.TimeUnit.MILLISECONDS))

    f.onComplete {
      case Success(f) =>
        onComplete
      case Failure(e) =>
        println("Sending failed to " + taskRef)
        val newTaskRef = Await.result(
          (masterRef ? ResolveMessage(taskId)).mapTo[ActorRef], DURATION)
        println("Retrieved new taskRef = " + newTaskRef)
        tasks(taskId) = newTaskRef

        sendToTask(taskId, message, round + 1)(onComplete)
    }
  }
}
