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

import akka.actor.ActorRef

import tbd.TBD
import tbd.worker.Worker

class Dest[T](worker: Worker, aModId: ModId) extends Mod[T](aModId) {
  val mod = new LocalMod[T](worker.self, id)
  worker.nextModId += 1

  def read(workerRef: ActorRef = null): T = {
    throw new RuntimeException("Not implemented.")
  }

  def update(value: T, workerRef: ActorRef, tbd: TBD): Int = {
    throw new RuntimeException("Not implemented.")
  }
}
