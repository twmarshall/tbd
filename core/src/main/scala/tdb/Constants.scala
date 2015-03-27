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

import akka.util.Timeout
import java.net.InetAddress
import scala.concurrent.duration._

object Constants {
  var DURATION = 10.seconds
  implicit var TIMEOUT = Timeout(DURATION)

  type WorkerId = Short

  // The first 16 bits of a TaskId represent the Worker the Task is running on.
  // This allows us to generate unique ids in parallel.
  type TaskId = Int

  // The first 32 bits of a ModId represent the TaskId where the Mod was
  // created. We can use this to request the value of the Mod from the Worker
  // that owns it.
  type ModId = Long

  type InputId = Int

  type NodeId = Int

  def createModId
      (datastoreId: TaskId,
       taskId: TaskId,
       nextModId: Int): ModId = {
    var newModId: Long = datastoreId
    newModId = newModId << 16
    newModId += taskId
    newModId = newModId << 16
    newModId += nextModId

    assert(getDatastoreId(newModId) == datastoreId)
    newModId
  }

  def getDatastoreId(modId: ModId): TaskId = {
    (modId >> 32).toInt
  }

  def incrementWorkerId(workerId: WorkerId): WorkerId = {
    (workerId + 1).toShort
  }

  val localhost = InetAddress.getLocalHost.getHostAddress

  val akkaConf = """
    akka.actor.provider = akka.remote.RemoteActorRefProvider

    akka.log-dead-letters = 1

    akka.remote.enabled-transports = [akka.remote.netty.tcp]
    akka.remote.log-remote-lifecycle-events = off
    akka.remote.netty.tcp.maximum-frame-size = 80000000b
  """

  val recordSeparator = 30.toChar
  val unitSeparator = 31.toChar
}
