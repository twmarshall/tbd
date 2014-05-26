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

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

object Main {
  val debug = false

  var id = 0

  def main(args: Array[String]) {
    val main = new Main()
    println("New master started at: akka.tcp://" + main.system.name + "@127.0.0.1:2552")
  }
}

class Main {
  val system = ActorSystem("masterSystem" + Main.id,
                           ConfigFactory.load.getConfig("master"))
  Main.id += 1

  val masterRef = system.actorOf(Master.props(), "master")

  def shutdown() {
    system.shutdown()
  }
}
