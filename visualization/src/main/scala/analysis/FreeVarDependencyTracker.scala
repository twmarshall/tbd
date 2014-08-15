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
 * Finds dependencies introduced by free variables which are bound from an
 * outer scope.
 */
class FreeVarDependencyTracker extends DependencyTracker {
  def findDependencies(ddg: DDG): Iterable[Edge] = {

    //Inverse direction for faster lookup
    var invDeps = HashMap[Node, List[Edge.FreeVar]]()

    //Insert single edges for each node.
    ddg.nodes.foreach(node => {
      var dependencies = node.tag match {
        case Tag.Read(_, fun) => fun.freeVars
        case Tag.Par(fun1, fun2) => fun1.freeVars ::: fun2.freeVars
        case Tag.Mod(_, fun) => fun.freeVars
        case Tag.Memo(fun, _) => fun.freeVars
        case _ => List[(String, Any)]()
      }

      if(!dependencies.isEmpty) {
        val parent = ddg.getCallParent(node)
        if(!invDeps.contains(parent)) {
          invDeps(parent) = List()
        }

        invDeps(parent) = Edge.FreeVar(node, parent, dependencies) :: invDeps(parent)
      }
    })

    //Finds adjacent paths with the same dependencies, and makes them
    //point to the node where the bound variable is introduced instead of some
    //intermediate node. This is comparable to path compression. 
    //
    //For example, the set of dependencies (A -> B) (B -> C) becomes
    //(A -> C) (B -> C).
    val iter = new TopoSortIterator(
                ddg.root,
                ddg,
                (e: Edge) => (e.isInstanceOf[Edge.Control])).toList
    iter.foreach(node => {
      val parent = ddg.getCallParent(node)

      if(parent != null && invDeps.contains(node)) {
        val depsToMe = invDeps(node)
        val depsToParent = invDeps(parent)

        for(depToMe <- depsToMe) {
          for(depToParent <- depsToParent) {
            if(depToParent.source == node) {
              val common = depToParent.dependencies.intersect(depToMe.dependencies)

              if(!common.isEmpty) {
                depToMe.dependencies = depToMe.dependencies.filter(x => {
                  !common.contains(x)
                })

                invDeps(parent) = Edge.FreeVar(depToMe.source, parent, common) ::
                                  invDeps(parent)
              }
            }
          }
        }
      }
    })

    //Remove dependencies to root - these are constant and bound from outside
    //of the program (like memoized or parallel parameters).
    invDeps(ddg.root) = List()

    //Add non-empty edges to graph
    invDeps.flatMap(dep => {
      dep._2.filter(x => !x.dependencies.isEmpty)
    })
  }
}
