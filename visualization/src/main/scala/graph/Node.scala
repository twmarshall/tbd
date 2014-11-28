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
package tbd.visualization.graph

import tbd.Constants._
import tbd.debug.TBD
import tbd.ddg

/*
 * Represents a visualizer DDG node.
 * Is created by copying all necessary properties of the TBD ddg node.
 */
class Node(time: Pointer) {

  val (internalId, tag, stacktrace) =
    TBD.nodes(ddg.Timestamp.getNodePtr(time))

  override def toString = {
    tag.toString()
    //ddgNode.toString
  }

  //Gets a string identifying the nodes type.
  def typeString(): String = {
    tag match {
      case x:ddg.Tag.Write => "write"
      case x:ddg.Tag.Read => "read"
      case x:ddg.Tag.Memo => "memo"
      case x:ddg.Tag.Par => "par"
      case x:ddg.Tag.Root => "root"
      case x:ddg.Tag.Mod => "mod"
    }
  }

  //Gets a short label representing this node. 
  def shortLabel(): String = {
    (tag match {
      case ddg.Tag.Write(writes) => writes
      case ddg.Tag.Read(value, fun) => (value, formatFunctionTag(fun))
      case ddg.Tag.Memo(fun, args) => (formatFunctionTag(fun), args)
      case ddg.Tag.Par(fun1, fun2) => {
        (formatFunctionTag(fun1), formatFunctionTag(fun2))
      }
      case ddg.Tag.Root() => ""
      case ddg.Tag.Mod(dest, fun) => (dest, formatFunctionTag(fun))
    }).toString()
  }

  private def formatFunctionTag(fun: ddg.FunctionTag): String = {
    "Fun@" + fun.funcId
  }
}
