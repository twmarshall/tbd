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
 * Trait for dependency trackers, which track dependencies and
 * insert them into the DDG as new edges.
 */
trait DependencyTracker {
  //Invokes findDependencies and inserts the result in the DDG.
  def findAndInsertDependencies(ddg: DDG) {
    var deps = findDependencies(ddg)

    deps.foreach(d => {
        ddg.adj(d.source) += d;
    })
  }

  //Scans the DDG for dependencies and returns the result as a list of edges. 
  def findDependencies(ddg: DDG): Iterable[Edge]
}
