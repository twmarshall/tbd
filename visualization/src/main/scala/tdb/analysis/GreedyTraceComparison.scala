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

package thomasdb.visualization.analysis

import thomasdb.visualization.graph._
import scala.collection.mutable.{Buffer, HashSet}

/*
 * Greedy (intrinsic) trace distance computation.
 */
class GreedyTraceComparison(extractor: (Node => Any))
  extends TraceComparison(extractor) {

  def compare(before: DDG, after: DDG):
      ComparisonResult = {

    //Inserts nodes into two sets A, B and computes
    //unchanged = A ? B
    //removed = A \ B
    //added = B \ A
    var set = after.nodes.map(x => new NodeWrapper(x, extractor))

    var removed = List[Node]()
    var added = List[Node]()
    var unchanged = List[Node]()

    before.nodes.map(x => new NodeWrapper(x, extractor)).foreach(x => {
      if(set.remove(x)) {
        unchanged = x.node :: unchanged
      } else {
        added = x.node :: added
      }
    })

    set.foreach(x => removed = x.node :: removed)

    new ComparisonResult(removed, added, unchanged)
  }
}
