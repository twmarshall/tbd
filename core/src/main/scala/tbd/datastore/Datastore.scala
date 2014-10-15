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
package tbd.datastore

import akka.actor.{Actor, ActorLogging, Props}
import scala.collection.mutable.Map

import tbd.Constants._
import tbd.messages._

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  val mods = Map[ModId, Any]()

  def receive = {
    case GetModMessage(modId: ModId) =>
      if (mods(modId) == null)
        sender ! NullMessage
      else
        sender ! mods(modId)

    case UpdateModMessage(modId: ModId, value: Any) =>
      mods(modId) = value

    case UpdateModMessage(modId: ModId, null) =>
      mods(modId) = null

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}
