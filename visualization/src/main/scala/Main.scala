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

package tbd.visualization

import org.graphstream.graph.implementations.{SingleGraph}

import scala.collection.mutable.ArrayBuffer
import tbd.{Adjustable, Changeable, Mutator, TBD}
import tbd.mod.{AdjustableList, Dest, Mod}
import tbd.ddg.{Node, RootNode, ReadNode, MemoNode, WriteNode, ParNode}

object Main {

  def graphStyle = """
    node.root {
      size: 20px;
      fill-color: orange;
    }
    edge {
      arrow-shape: arrow;
    }
    node.read{
      size: 10px;
      fill-color: blue;
    }
    node.write {
      size: 10px;
      fill-color: red;
    }
    node.memo {
      size: 10px;
      fill-color: green;
    }
    node.par {
      size: 10px;
      fill-color: yellow;
    }
    node {
      text-alignment: under;
      text-background-color: #C0C0C0;
      text-background-mode: rounded-box;
    }
  """

  def main(args: Array[String]) {

    System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")

    val mutator = new Mutator()
    mutator.put("one", 0)
    mutator.put("two", 3)
    mutator.put("three", 2)
    mutator.put("four", 4)
    mutator.put("five", 1)
    val output = mutator.run[AdjustableList[String, Int]](new ListSortTest())

    output.toBuffer().foreach(x => { print(x); print(" ") })
    println()
    showDDG(mutator.getDDG().root)

    mutator.put("six", 5)
    mutator.propagate()
    output.toBuffer().foreach(x => { print(x); print(" ") })
    println()

    mutator.put("seven", -1)
    mutator.propagate()
    output.toBuffer().foreach(x => { print(x); print(" ") })
    println()

    mutator.update("two", 5)
    mutator.propagate()
    output.toBuffer().foreach(x => { print(x); print(" ") })
    println()

    mutator.shutdown()
  }

  def showDDG(node: RootNode) = {
    val graph = new SingleGraph("DDG")
    graph.addAttribute("ui.stylesheet", graphStyle);

    val n:org.graphstream.graph.Node = graph.addNode(System.identityHashCode(node).toString())
    n.addAttribute("ui.label", "root")
    n.addAttribute("ui.class", "root")
    var cx = 0.asInstanceOf[AnyRef]
    var cy = 0.asInstanceOf[AnyRef]
    var cz = 0.asInstanceOf[AnyRef]
    n.setAttribute("xyz", cx, cy, cz);


    showDDGTraverse(graph, node, 1, 0)

    var display = graph.display()
    display.disableAutoLayout()
  }

  def showDDGTraverse(graph: SingleGraph, node: Node, depth: Int, span: Int): Int = {

    var lspan = span;

    if(node.children.length == 0)
      return lspan + 1

    var lastWasWrite = true
    node.children.foreach(x => {

      val n:org.graphstream.graph.Node = graph.addNode(System.identityHashCode(x).toString())

      var desc = ""

      var cx = (lspan * 50)
      var cy = (depth * -10)
      var cz = 0
      var nodeClass = x match {
        case x:WriteNode =>
             desc = x.mod.toString().replace("DoubleModListNode", "DMLN")
             "write"
        case x:ReadNode =>
             desc = x.mod.toString().replace("DoubleModListNode", "DMLN")
             "read"
        case x:MemoNode => "memo"
        case x:ParNode => "par"
      }

      n.setAttribute("xyz", cx.asInstanceOf[AnyRef], cy.asInstanceOf[AnyRef], cz.asInstanceOf[AnyRef]);

      val methodNames = x.stacktrace.map(y => y.getMethodName())
      var currentMethod = methodNames.filter(y => (!y.startsWith("<init>")
                                              && !y.startsWith("addRead")
                                              && !y.startsWith("addWrite")
                                              && !y.startsWith("createMod")
                                              && !y.startsWith("getStackTrace")
                                              && !y.startsWith("liftedTree")
                                              && !y.startsWith("apply")
                                              && !y.startsWith("read")
                                              && !y.startsWith("write")
                                              && !y.startsWith("mod")))(0)

      if(methodNames.find(x => x == "createMod").isDefined) {
        currentMethod += " (createMod)"
      }

      n.addAttribute("ui.class", nodeClass)
      n.addAttribute("ui.label", "  " + nodeClass + " " + desc + " in " + currentMethod)
      graph.addEdge(System.identityHashCode(node).toString() + "->" + System.identityHashCode(x).toString(), System.identityHashCode(node).toString(), System.identityHashCode(x).toString())

      lspan = showDDGTraverse(graph, x, depth + 1, lspan)
    })

  lspan
  }
}

class ListSortTest() extends Adjustable {
  def run(tbd: TBD): (AdjustableList[String, Int]) = {
    val list = tbd.input.getAdjustableList[String, Int](partitions = 1)
    list.sort(tbd, (tbd, a, b) => {
        a._2 < b._2
    })
  }
}

