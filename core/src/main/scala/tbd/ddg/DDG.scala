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
package tbd.ddg

import akka.actor.Actor
import akka.actor.ActorLogging

import tbd.messages._
import tbd.mod.ModId

trait DDG extends Actor with ActorLogging {
  def addRead(modId: ModId, readId: ReadId)

  def addWrite(readId: ReadId, modId: ModId)

  def addCall(outerCall: ReadId, innerCall: ReadId)

  def receive = {
    case AddReadMessage(modId: ModId, readId: ReadId) =>
      addRead(modId, readId)
    case AddWriteMessage(readId: ReadId, modId: ModId) =>
      addWrite(readId, modId)
    case AddCallMessage(outerCall: ReadId, innerCall: ReadId) =>
      addCall(outerCall, innerCall)
    case ToStringMessage => sender ! toString
  }
}
