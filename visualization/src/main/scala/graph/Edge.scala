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

import tbd.Constants.ModId

/*
 * Represents an edge in a DDG.
 */
abstract class Edge() {
  def source: Node
  def destination: Node
}
object Edge {
  /*
   * Represents a control edge, pointing from a parent to its child in
   * the trace.
   */
  case class Control(val source: Node, val destination: Node) extends Edge
  /*
   * Represents a reverse control edgeG, pointing from a child to its
   * parent in the trace.
   */
  case class InverseControl(val source: Node, val destination: Node)
    extends Edge
  /*
   * Represents a read -> write edge, pointing from a read node to the write
   * node where the corresponding mod was written.
   */
  case class ReadWrite(val source: Node, val destination: Node, val modId: ModId)
    extends Edge
  /*
   * Represents a write -> read edge, pointing from a write node to the read
   * node where the corresponding mod was read.
   */
  case class WriteRead(val source: Node, val destination: Node, val modId: ModId)
    extends Edge
  /*
   * Represents a mod -> write edge, pointing from a mod node to the write
   * node where the corresponding mod was written.
   */
  case class ModWrite(val source: Node, val destination: Node, val modId: ModId)
    extends Edge
  /*
   * Represents a write -> mod edge, pointing from a write node to the mod
   * node where the corresponding mod was created.
   */
  case class WriteMod(val source: Node, val destination: Node, val modId: ModId)
    extends Edge
  /*
   * Represents a free war edge, pointing from a any node to the node where
   * the variable is originally bound from.
   */
  case class FreeVar(
      val source: Node,
      val destination: Node,
      //Dependency list contains of the name and the value of the bound
      //variable.
      var dependencies: List[(String, Any)])
    extends Edge
}