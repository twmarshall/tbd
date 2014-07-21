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

import tbd.visualization.graph._
import tbd.ddg.{Tag, FunctionTag}
import scala.collection.mutable.{HashMap, ListBuffer}
import swing._
import swing.event._
import java.awt.{Color, Graphics2D}

class TbdVisualizer extends Panel with Publisher {

  val vSpacing = 50
  val hSpacing = 50

  var dx: Float = 20
  var dy: Float = 20

  var sx: Float = 1
  var sy: Float = 1

  var lx = -1
  var ly = -1

  val pos = new HashMap[Node, (Int, Int)]()
  val nodes = new ListBuffer[Node]()
  val edges = new ListBuffer[(Node, Node)]()

  listenTo(this.mouse.moves)
  listenTo(this.mouse.clicks)
  listenTo(this.mouse.wheel)
  listenTo(this.keys)
  focusable = true

  private def setPos(node: Node, x: Int, y: Int) {
    pos(node) = (x, y)
  }

  private def getPos(node: Node): (Int, Int) = {
    if(pos.contains(node)) {
      pos(node)
    } else {
      (0, 0)
    }
  }

  private def clear() {
    edges.clear()
    nodes.clear()
    pos.clear()
  }

  private def transform(pos: (Int, Int)): (Int, Int) = {
    val (x, y) = pos

    (((x + dx) * sx).toInt, ((y + dy) * sy).toInt)
  }

  override def paintComponent(g: Graphics2D) = {

    g.setColor(Color.WHITE)
    g.fillRect(0, 0, g.getClipBounds().width , g.getClipBounds().height)

    for(edge <- edges) {
      val (x1, y1) = transform(getPos(edge._1))
      val (x2, y2) = transform(getPos(edge._2))

      g.setColor(Color.BLACK)
      g.drawLine(x1, y1, x2, y2)
    }

    for(node <- nodes) {
      val (x, y) = transform(getPos(node))

      node.tag match {
        case q:Tag.Read => drawRead(x, y, g)
        case q:Tag.Write => drawWrite(x, y, g)
        case q:Tag.Memo => drawMemo(x, y, g)
        case q:Tag.Par => drawPar(x, y, g)
        case q:Tag.Root => drawRoot(x, y, g)
        case q:Tag.Mod => drawMod(x, y, g)
      }
    }
  }

  private def drawRead(x: Int, y: Int, g: Graphics2D) {
    val radius = 5

    g.setColor(Color.BLUE)
    g.fillOval(x - radius, y - radius, radius * 2, radius * 2)
  }

  private def drawWrite(x: Int, y: Int, g: Graphics2D) {
    val radius = 5

    g.setColor(Color.ORANGE)
    g.fillOval(x - radius, y - radius, radius * 2, radius * 2)
  }

  private def drawMod(x: Int, y: Int, g: Graphics2D) {
    val radius = 5

    g.setColor(Color.MAGENTA)
    g.fillOval(x - radius, y - radius, radius * 2, radius * 2)
  }

  private def drawRoot(x: Int, y: Int, g: Graphics2D) {
    val size = 5

    g.setColor(Color.GRAY)
    g.fillRect(x - size, y - size, size * 2, size * 2)
  }

  private def drawMemo(x: Int, y: Int, g: Graphics2D) {
    val size = 5

    val transform = g.getTransform()
    g.translate(x, y)
    g.rotate(Math.PI / 4)
    g.setColor(Color.GREEN)
    g.fillRect(-size, -size, size * 2, size * 2)
    g.setTransform(transform)
  }

  private def drawPar(x: Int, y: Int, g: Graphics2D) {
    val size = 5

    val transform = g.getTransform()
    g.translate(x, y)
    g.rotate(Math.PI / 4)
    g.setColor(Color.YELLOW)
    g.fillRect(-size, -size, size * 2, size * 2)
    g.setTransform(transform)
  }

  reactions += {
    case e: KeyPressed => println("Key pressed" + e)
    case e: MousePressed => null
    case MouseDragged(_, point, _) => {
        if(lx != -1 && ly != -1) {
          dx -= (lx - point.x) / sx
          dy -= (ly - point.y) / sy
          println("Drag")
        }
        lx = point.x
        ly = point.y

        repaint()
    }
    case e: MouseReleased => {
      lx = -1
      ly = -1
    }
    case MouseWheelMoved(_, _, _, dir) => {

      val scaleSpeed = 1.3f

      if(dir < 0) {
        sx *= scaleSpeed
        sy *= scaleSpeed
      } else {
        sx /= scaleSpeed
        sy /= scaleSpeed
      }

      repaint()
    }
    case _ => println ("Unreacted event")
  }

  private def addNode(node: Node) = {
    nodes += node
  }

  private def removeNode(node: Node) {
    nodes -= node
  }

  private def addEdge(a: Node, b: Node) = {
    edges += Tuple2(a, b)
  }

  private def removeEdge(a: Node, b: Node) {
    edges -= Tuple2(a, b)
  }

  private def getNodeType(node: Node): String = {
    node.tag match {
      case x:Tag.Write => "write"
      case x:Tag.Read => "read"
      case x:Tag.Memo => "memo"
      case x:Tag.Par => "par"
      case x:Tag.Root => "root"
    }
  }

  def createTree(node: Node, parent: Node) {

    addNode(node)

    if(parent != null) {
      addEdge(parent, node)
    }

    ddg.getCallChildren(node).foreach(x => {
      createTree(x, node)
    })
  }

  //Layouts the tree and returns the subtree width.
  def layoutTree(parent: Node, depth: Int): Int = {

    setPos(parent, 0, depth * vSpacing)

    if(ddg.getCallChildren(parent).length == 0) {
      return 0
    }
    else {
      var sum = 0;
      ddg.getCallChildren(parent).foreach(child => {
          val width = layoutTree(child, depth + 1)
          if(sum != 0)
            translateTree(child, sum)

          sum += (width + hSpacing)
      })

      sum -= hSpacing

      setPos(parent, sum / 2, depth * vSpacing)

      sum
    }
  }

  def translateTree(parent: Node, dx: Int) {

    val (x, y) = getPos(parent)
    setPos(parent, x + dx, y)

    if(((x + dx) / vSpacing) % 2 == 0) {
      //setStyle(parent, "text-offset: 0, 3;")
    } else {
      //setStyle(parent, "text-offset: 0, -27;")
    }

    ddg.getCallChildren(parent).foreach(child => {
      translateTree(child, dx)
    })
  }

  var ddg: DDG = null

  def showDDG(ddg: DDG) = {

    clear()

    val root = ddg.root
    this.ddg = ddg


    createTree(root, null)
    layoutTree(root, 0)

    this.repaint()
  }

  private def extractMethodName(node: Node): String = {

    if(node.stacktrace == null) {
      return "<No stacktrace available. Set Main.debug = true to enable stacktraces>"
    }

    val methodNames = node.stacktrace.map(y => y.getMethodName())
    val fileNames = node.stacktrace.map(y => (y.getMethodName(), y.getFileName(), y.getLineNumber()))

    var (_, fileName, lineNumber) = fileNames.filter(y => (y._1.contains("apply")))(0)

    var currentMethod = methodNames.filter(y => (!y.startsWith("<init>")
                                            && !y.startsWith("()")
                                            && !y.startsWith("addRead")
                                            && !y.startsWith("addWrite")
                                            && !y.startsWith("addMemo")
                                            && !y.startsWith("createMod")
                                            && !y.startsWith("getStackTrace")
                                            && !y.startsWith("apply")
                                            && !y.startsWith("read")
                                            && !y.startsWith("memo")
                                            && !y.startsWith("par")
                                            && !y.startsWith("write")
                                            && !y.startsWith("mod")))(0)

    if(currentMethod.contains("$")) {
      currentMethod = currentMethod.substring(0, currentMethod.lastIndexOf("$"))
      currentMethod = currentMethod.substring(currentMethod.lastIndexOf("$") + 1)
    }

    if(methodNames.find(x => x == "createMod").isDefined) {
      currentMethod += " (createMod)"
    }

    currentMethod + " " + fileName + ":" + lineNumber.toString
  }

  def viewClosed(id: String) {

  }

  def buttonPushed(id: String) { //:(
      //val node = idToNodes(id)

      //publish(new NodeClickedEvent(node))
  }

  def formatTag(tag: Tag): String = {
    tag match{
      case Tag.Read(value, funcTag) => {
          "Read " + value +
          "\nReader " + formatFunctionTag(funcTag)
      }
      case Tag.Write(value, dest) => {
          "Write " + value + " to " + dest
      }
      case Tag.Memo(funcTag, signature) => {
          "Memo" +
          "\n" + formatFunctionTag(funcTag) +
          "\nSignature:" + signature.foldLeft("")(_ + "\n   " + _)
      }
      case Tag.Mod(dest, initializer) => {
          "Mod id " + dest
          "\nInitializer " + formatFunctionTag(initializer)
      }
      case Tag.Par(fun1, fun2) => {
          "Par" +
          "\nFirst " + formatFunctionTag(fun1) +
          "\nSecond " + formatFunctionTag(fun2)
      }
      case _ => ""
    }
  }

  def formatFunctionTag(tag: FunctionTag): String = {
    "Function: " + tag.funcId +
    "\nBound free vars:" +
    tag.freeVars.map(x => x._1 + " = " + x._2).foldLeft("")(_ + "\n   " + _)
  }

  def buttonReleased(id: String) {

  }
}

 class NodeClickedEvent(val node: Node) extends event.Event {}