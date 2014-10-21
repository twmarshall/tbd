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
package tbd

import akka.pattern.ask
import scala.concurrent.Await

import tbd.Constants._
import tbd.messages.RunTaskMessage
import tbd.worker.Task

class Parizer[T](one: Context => T) {
  def and[U](two: Context => U)(implicit c: Context): (T, U) = {
    val id1 = c.id + "-" + c.taskId
    val taskProps1 = Task.props(id1, c.task.self, c.datastore)
    val taskRef1 = c.task.context.system.actorOf(taskProps1, c.id + "-" + c.taskId)
    c.taskId += 1

    val adjust1 = new Adjustable[T] { def run(implicit c: Context) = one(c) }
    val oneFuture = taskRef1 ? RunTaskMessage(adjust1)

    val id2 = c.id + "-" + c.taskId
    val taskProps2 = Task.props(id2, c.task.self, c.datastore)
    val taskRef2 = c.task.context.system.actorOf(taskProps2, c.id + "-" + c.taskId)
    c.taskId += 1

    val adjust2 = new Adjustable[U] { def run(implicit c: Context) = two(c) }
    val twoFuture = taskRef2 ? RunTaskMessage(adjust2)

    val parNode = c.ddg.addPar(taskRef1, taskRef2, c)
    parNode.endTime = c.ddg.nextTimestamp(parNode, c)

    val oneRet = Await.result(oneFuture, DURATION).asInstanceOf[T]
    val twoRet = Await.result(twoFuture, DURATION).asInstanceOf[U]
    (oneRet, twoRet)
  }
}
