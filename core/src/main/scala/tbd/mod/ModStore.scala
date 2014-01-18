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
package tbd.mod

import akka.actor.{Actor, ActorLogging}
import scala.collection.mutable.Map

import tbd.messages._

class ModStore extends Actor with ActorLogging {
  val mods = Map[ModId, Any]()

  def read(modId: ModId): Any = {
    mods(modId)
  }

  def write(value: Any): ModId = {
    val modId = new ModId()
    mods += (modId -> value)
    modId
  }

  def receive = {
    case ReadModMessage(modId: ModId) => {
      val ret = read(modId)
      if (ret == null) {
	      sender ! NullValueMessage
      } else {
	      sender ! ret
      }
    }
    case WriteModMessage(value: Any) => sender ! write(value)
    case WriteNullModMessage => sender ! write(null)
    case _ => log.warning("ModStore actor received unkown message from " + sender)
  }
}
