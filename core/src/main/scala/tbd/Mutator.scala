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
import akka.util.Timeout
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import tbd.input.MutatorInput
import tbd.manager.LocalManager
import tbd.messages.RunMessage
import tbd.mod.Mod

class Mutator[T <: TBD: ClassManifest] {
  val manager = new LocalManager()
  manager.launchInput()
  manager.launchModStore()
  val input = new MutatorInput(manager)

  def run(): Output = {
    val masterRef = manager.launch()

    implicit val timeout = Timeout(5 seconds)
    val constructor = classManifest[T].erasure.getConstructor()
    val tbd = constructor.newInstance().asInstanceOf[TBD]
    val future = masterRef ? RunMessage(tbd)
    val resultFuture =
      Await.result(future, timeout.duration).asInstanceOf[Future[Mod[Any]]]

    new Output(Await.result(resultFuture, timeout.duration))
  }

  def shutdown() {
    manager.shutdown()
  }
}
