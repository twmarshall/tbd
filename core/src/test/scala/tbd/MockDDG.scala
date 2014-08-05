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
package tbd.test

import tbd.ddg.{MemoNode, Node, ReadNode, RootNode, ModNode}

object MockNode {
  var time: Int = 0
}

class MockNode(aChildren: List[MockNode]) {
  val time = MockNode.time
  MockNode.time += 1

  val children =  aChildren

  def isEqual(node: Node): Boolean = {
    var equal = true
    assert(node.children.size == children.size)
    for (i <- 0 until node.children.size) {
      equal &= children(i).isEqual(node.children(i))
    }

    this match {
      case MockRootNode(_) => {
        return equal && node.isInstanceOf[RootNode]
      }
      case MockMemoNode(_) => {
        return equal && node.isInstanceOf[MemoNode]
      }
      case MockReadNode(_) => {
        return equal && node.isInstanceOf[ReadNode]
      }
      case MockModNode(_) => {
        return equal && node.isInstanceOf[ModNode]
      }
    }
  }
}

case class MockRootNode(aChildren: List[MockNode]) extends MockNode(aChildren)
case class MockMemoNode(aChildren: List[MockNode]) extends MockNode(aChildren)
case class MockReadNode(aChildren: List[MockNode]) extends MockNode(aChildren)
case class MockModNode(aChildren: List[MockNode]) extends MockNode(aChildren)
