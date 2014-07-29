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

object DependencyTracker {

  def findAndInsertReadWriteDependencies(ddg: DDG) {
    val writes = new HashMap[ModId, Node]

    ddg.nodes.foreach(x => x.tag match {
      case y:Tag.Write => writes(y.dest) = x
      case _ => null
    })

    ddg.nodes.foreach(x => x.tag match {
      case y:Tag.Read => if(writes.contains(y.mod)) {
        val dst = writes(y.mod)
        val src = x
        ddg.adj(src) += Edge.ReadWrite(src, dst, y.mod)
        ddg.adj(dst) += Edge.WriteRead(dst, src, y.mod)
      }
      case _ => null
    })
  }

  def findAndInsertModWriteDependencies(ddg: DDG) {
    val mods = new HashMap[ModId, Node]

    ddg.nodes.foreach(x => x.tag match {
      case y:Tag.Mod => mods(y.dest) = x
      case _ => null
    })

    ddg.nodes.foreach(x => x.tag match {
      case y:Tag.Write => if(mods.contains(y.dest)) {
        val dst = mods(y.dest)
        val src = x
        ddg.adj(src) += Edge.ModWrite(src, dst, y.dest)
        ddg.adj(dst) += Edge.WriteMod(dst, src, y.dest)
      }
      case _ => null
    })
  }
  def findAndInsertFreeVarDependencies(ddg: DDG) {

    //Inverse direction for faster lookup
    var invDeps = HashMap[Node, List[Edge.FreeVar]]()

    //Find all dependencies (only between child/parent

    ddg.adj.flatMap(x => {
      x._2.filter(y => y.isInstanceOf[Edge.Control])
    }).map(e => {
      e.destination.tag match {
        case Tag.Read(_, fun) => Edge.FreeVar(e.destination, e.source, fun.freeVars)
        case Tag.Memo(fun, _) => Edge.FreeVar(e.destination, e.source, fun.freeVars)
        case Tag.Mod(_, fun) => Edge.FreeVar(e.destination, e.source, fun.freeVars)
        case Tag.Par(fun1, fun2) => Edge.FreeVar(e.destination, e.source, fun1.freeVars ::: fun2.freeVars)
        case _ => null
      }
    }).filter(x => x != null).foreach(e => {
      invDeps(e.destination) = List(e)
    })

    //Bubble dependencies upwards
    val iter = new TopoSortIterator(
          ddg.root,
          ddg,
          (e: Edge) => (e.isInstanceOf[Edge.Control])).toList

    iter.foreach(n => {
      if(!n.tag.isInstanceOf[Tag.Root] && invDeps.contains(n)) {
        val vars = n.tag match {
          case Tag.Read(_, fun) => fun.freeVars
          case Tag.Memo(fun, _) => fun.freeVars
          case Tag.Mod(_, fun) => fun.freeVars
          case Tag.Par(fun1, fun2) => fun1.freeVars ::: fun2.freeVars
          case _ => List[(String, Any)]()
        }

        invDeps(n).foreach(e => {
          val inter = vars.intersect(e.dependencies)

          e.dependencies = e.dependencies.filter(e => !inter.contains(e))

          val parent = ddg.getCallParent(n)

          invDeps(parent) = (Edge.FreeVar(e.source, parent, inter)) :: invDeps(parent)
        })
      }
    })

    //Merge and reduce freeVar dep edges
    ddg.nodes.foreach(n => {
      if(invDeps.contains(n)) {
        var deps = HashMap[Node, Edge.FreeVar]()
        invDeps(n).foreach(e =>
          if(deps.contains(e.source)) {
            deps(e.source).dependencies = (deps(e.source).dependencies ::: e.dependencies).distinct
          } else (
            deps(e.source) = Edge.FreeVar(e.source, e.destination, e.dependencies.distinct)
          )
        )

        //Remove empty dependencies and dependencies to direct childs of root
        //because this is out of our scope of change propagation and therefore
        //considered constant.
        deps = deps.filter(e => {
          !e._2.dependencies.isEmpty &&
          e._2.destination != ddg.root &&
          ddg.getCallParent(e._2.destination) != ddg.root
        })

        deps.foreach(e => {
          ddg.adj(e._2.source) += e._2
        })
      }
    })
  }
}
