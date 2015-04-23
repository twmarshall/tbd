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
package tdb.ddg

import akka.pattern.ask
import java.io._
import scala.collection.mutable
import scala.concurrent.Await

import tdb.Constants._
import tdb.messages._

class DDGPrinter
    (ddg: DDG,
     _nextName: Int,
     output: BufferedWriter) {

  var nextName = _nextName

  val names = mutable.Map[Node, Int]()

  def print(): Int = {
    dotsHelper(ddg.root, output)

    nextName
  }

  private def dotsHelper(time: Timestamp, output: BufferedWriter) {
    val name = getName(time.node)

    time.node match {
      case readNode: ReadNode =>
        writeShape(name, "box", "read")
      case parNode: ParNode =>
        writeShape(name, "triangle", "par")
      case getNode: GetNode =>
        writeShape(name, "square", "get")
      case putNod: PutNode =>
        writeShape(name, "circle", "put")
      case putAllNode: PutAllNode =>
        writeShape(name, "circle", "putAll")
      case memoNode: MemoNode =>
        writeShape(name, "diamond", "memo")
      case rootNode: RootNode =>
        writeShape(name, "invtriangle", "root")
      case _ =>
        println("didn't match " + time.node.getClass)
    }

    time.node match {
      case parNode: ParNode =>
        output.write(name + " -> " + nextName + "\n")
        nextName = Await.result(
          (parNode.taskRef1 ? PrintDDGDotsMessage(nextName, output)).mapTo[Int],
          DURATION)
        output.write(name + " -> " + nextName + "\n")
        nextName = Await.result(
          (parNode.taskRef2 ? PrintDDGDotsMessage(nextName, output)).mapTo[Int],
          DURATION)
      case _ =>
        for (child <- ddg.ordering.getChildren(time, time.end)) {
          val childName = getName(child.node)

          output.write(name + " -> " + childName + "\n")
          dotsHelper(child, output)
        }
    }
  }

  private def getName(node: Node) =
    if (names.contains(node)) {
      names(node)
    } else {
      names(node) = nextName
      nextName += 1
      nextName - 1
    }

  private def writeShape(name: Int, shape: String, label: String) {
    output.write(name + s" [shape=$shape label=$label]\n")
  }
}
