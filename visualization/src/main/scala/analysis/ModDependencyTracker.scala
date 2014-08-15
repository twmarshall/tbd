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

class ModDependencyTracker extends DependencyTracker {
  def findDependencies(ddg: DDG): Iterable[Edge] = {
    val mods = new HashMap[ModId, Node]

    ddg.nodes.foreach(x => x.tag match {
      case Tag.Mod(dests, _) => for(dest <- dests) {
          mods(dest) = x
      }
      case _ => null
    })

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