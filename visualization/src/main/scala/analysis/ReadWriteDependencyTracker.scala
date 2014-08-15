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
package tbd.visualization.analysis

import scala.collection.mutable.{HashMap}
import tbd.ddg.Tag
import tbd.Constants.ModId
import tbd.visualization.graph._

/*
 * Finds all dependencies between write nodes and their corresponding read
 * nodes in the DDG.
 */
class ReadWriteDependencyTracker extends DependencyTracker {
  def findDependencies(ddg: DDG): Iterable[Edge] = {

    //Find all write nodes and remember them.
    val writes = new HashMap[ModId, Node]

    ddg.nodes.foreach(x => x.tag match {
      case Tag.Write(ws) => for(w <- ws) {
        writes(w.mod) = x
      }
      case _ => null
    })

    //Find all read nodes. If we find a read node whicih reads one
    //of the remembered mods, we add an edge to the result.
    ddg.nodes.flatMap(x => x.tag match {
      case y:Tag.Read if writes.contains(y.mod) =>
        val dst = writes(y.mod)
        val src = x
        List(Edge.ReadWrite(src, dst, y.mod), Edge.WriteRead(dst, src, y.mod))
      case _ => List()
    })
  }
}