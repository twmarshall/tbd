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
package tdb.worker

import com.datastax.driver.core.Cluster

import tdb.Constants._
import tdb.util.OS

case class WorkerInfo
  (workerId: TaskId,
   systemName: String,
   ip: String,
   port: Int,
   webuiPort: Int,
   storeType: String,
   envHomePath: String,
   cacheSize: Int,
   mainDatastoreId: TaskId = -1,
   numCores: Int = OS.getNumCores(),
   cluster: Cluster = null) {
  val workerAddress = "akka.tcp://" + systemName + "@" + ip + ":" +
    port + "/user/worker"

  val webuiAddress = port + ":" + webuiPort
}
