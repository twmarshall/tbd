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

package tdb.visualization.analysis

import scala.collection.mutable.{HashMap}
import tdb.ddg.Tag
import tdb.Constants.ModId
import tdb.visualization.graph._

/*
 * Finds all dependencies between mod nodes and their corresponding write
 * nodes in the DDG.
 */
class ModDependencyTracker extends DependencyTracker {
  def findDependencies(ddg: DDG): Iterable[Edge] = {

    //Find all mod nodes and remember them.
    val mods = new HashMap[ModId, Node]

    ddg.nodes.foreach(x => x.tag match {
      case Tag.Mod(dests, _) => for(dest <- dests) {
          mods(dest) = x
      }
      case _ => null
    })

    //Find all write nodes. If we find a write node which writes to one
    //of the remembered mods, we add an edge to the result. 
    ddg.nodes.flatMap(x => x.tag match {
      case Tag.Write(writes) =>
        writes.flatMap(write => {
          if(mods.contains(write.mod)) {
            val dst = mods(write.mod)
            val src = x
            List(Edge.WriteMod(src, dst, write.mod),
                 Edge.ModWrite(dst, src, write.mod))
          } else {
            List()
          }
        })
      case _ => List()
    })
  }
}
