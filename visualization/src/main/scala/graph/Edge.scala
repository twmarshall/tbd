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

abstract class Edge() {
  def source: Node
  def destination: Node
}
object Edge {
  case class Control(val source: Node, val destination: Node) extends Edge
  case class InverseControl(val source: Node, val destination: Node) extends Edge
  case class ReadWrite(val source: Node, val destination: Node, val modId: ModId) extends Edge
  case class WriteRead(val source: Node, val destination: Node, val modId: ModId) extends Edge
  case class ModWrite(val source: Node, val destination: Node, val modId: ModId) extends Edge
  case class WriteMod(val source: Node, val destination: Node, val modId: ModId) extends Edge
  case class FreeVar(val source: Node, val destination: Node, var dependencies: List[(String, Any)]) extends Edge
}