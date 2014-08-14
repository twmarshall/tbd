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
import tbd.visualization.graph._
import tbd.visualization.ExperimentResult

class IndependentSubtrees extends TraceAnalysis {
  def analyze(trace: ExperimentResult[Any]): Iterable[AnalysisInfo] = {
    val ddg = trace.ddg

    val deps = HashMap[Node, List[Node]]()
    val subtree = HashMap[Node, List[Node]]()

    // (NodeType, Method, NodeType, method)
    type DepTag = (String, MethodInfo, String, MethodInfo)
    //Note: Binary deps might be not sufficient. A dependency graph would
    //be better (we could use tracing of changes).
    //Furthermore, this does not work for stuff which is not mod <- Therefore, we should ignore subtrees without write. Write guarantees to give mod  back.
    var calls = HashMap[DepTag, Boolean]()

    val iter = new TopoSortIterator(
                ddg.root,
                ddg,
                (e: Edge) => (e.isInstanceOf[Edge.Control])).toList

    for(node <- iter) {
      val nodeDeps = ddg.adj(node).filter(e => {
        e.isInstanceOf[Edge.WriteRead] || e.isInstanceOf[Edge.ReadWrite]
      })

      if(!deps.contains(node)) {
        deps(node) = List()
      }

      for(nodeDep <- nodeDeps) {
        deps(node) = nodeDep.destination :: deps(node)
      }

      val children = ddg.adj(node).filter(e => {
        e.isInstanceOf[Edge.Control]
      }).map(_.destination)

      if(!node.tag.isInstanceOf[tbd.ddg.Tag.Par]) {
        val childrenToIterate = children.filter(c => {
          !c.tag.isInstanceOf[tbd.ddg.Tag.Write] &&
          !c.tag.isInstanceOf[tbd.ddg.Tag.Par]
        })

        for(a <- childrenToIterate) {
          for(b <- childrenToIterate) {
            if(a != b && !deps(a).isEmpty && !deps(b).isEmpty) { //Empty trees are not interesting...

              val callTag = (a.typeString, MethodInfo.extract(a),
                             b.typeString, MethodInfo.extract(b))

              if(!calls.contains(callTag))
                calls(callTag) = true

              val isDepFree = deps(a).forall(x => !subtree(b).contains(x)) &&
                              deps(b).forall(x => !subtree(a).contains(x))

              calls(callTag) = calls(callTag) && isDepFree
            }
          }
        }
      }

      subtree(node) = List(node)
      for(child <- children) {
        deps(node) = deps(node) ::: deps(child)
        subtree(node) = subtree(node) ::: subtree(child)
      }
    }

    calls.filter(_._2).map(_._1).map(x => {
      if(x._2.line < x._4.line) {
        x
      } else {
        (x._3, x._4, x._1, x._2)
      }
    }).toStream.distinct.map(x => {
      IndependentSubtreeInfo(
        s"Call trees of ${x._1} in ${x._2.file}:${x._2.line} and ${x._3} " +
        s"in ${x._4.file}:${x._4.line} are independent for this test.",
        x._2.file, x._2.line, x._4.line)
    }).toBuffer.sortWith(_.message < _.message)
  }
}
