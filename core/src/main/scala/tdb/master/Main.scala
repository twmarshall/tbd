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
package tdb.master

import akka.util.Timeout
import java.net.InetAddress
import scala.concurrent.duration._

import tdb.Constants
import tdb.stats.Stats

object Main {
  def main(args: Array[String]) {
    val conf = new MasterConf(args)

    Constants.DURATION = conf.timeout().seconds
    Constants.TIMEOUT = Timeout(Constants.DURATION)

    val connector = MasterConnector(masterConf = conf, singleNode = false)
    println("New master started at: akka.tcp://" + connector.system.name +
            "@" + conf.ip() + ":" + conf.port() + "/user/master")

    Stats.launch(connector.system, "master", conf.ip(), conf.webui_port())
  }
}

