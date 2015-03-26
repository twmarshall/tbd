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
package tdb.scripts

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.mashupbots.socko.events.HttpRequestEvent
import org.mashupbots.socko.routes._
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}
import org.rogach.scallop._

import tdb.Constants._
import tdb.util._

object GraphServer {
  def props() = Props(classOf[GraphServer])

  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("TDB 0.1 (c) 2014 Carnegie Mellon University")
      val ip = opt[String]("ip", default = Some(Util.getIP()))
      val port = opt[Int]("port", default = Some(8082))
    }

    val ip = Conf.ip()
    val port = Conf.port()
    val conf = akkaConf + s"""
      akka.remote.netty.tcp.hostname = $ip
      akka.remote.netty.tcp.port = 2554
    """

    val system = ActorSystem("GraphServer",
      ConfigFactory.load(ConfigFactory.parseString(conf)))

    val actorRef = system.actorOf(GraphServer.props())

    val routes = Routes({
      case HttpRequest(request) => request match {
        case GET(request) =>
          actorRef ! request
      }
    })

    val webServer =
      new WebServer(
        WebServerConfig(hostname = ip, port = port),
        routes,
        system)

    webServer.start()
  }
}

class GraphServer extends Actor {
  def receive = {
    case request: HttpRequestEvent =>
      request match {
        case GET(Path(path)) =>
          request.response.write(FileUtil.getBytes("graphs/" + path), "image/png")
      }
    case _ =>
  }
}
