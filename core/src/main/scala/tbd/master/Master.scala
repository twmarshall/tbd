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
package tbd.master

import akka.actor.{Actor, ActorLogging}
import scala.concurrent.Promise

import tbd.Destination
import tbd.Output
import tbd.TBD
import tbd.manager.Manager
import tbd.messages._
import tbd.mod.Mod

class Master(tbd: TBD, manager: Manager, resultPromise: Promise[Output])
    extends Actor with ActorLogging {
  log.info("Master launced.")
  val firstInitialWorker =
    manager.launchInitialWorker(() => tbd.run(new Destination))

  def receive = {
    case FinishedMessage(result) =>
      resultPromise.success(new Output(result.read()))
    case _ => log.warning("Master actor received unkown message!")
  }
}
