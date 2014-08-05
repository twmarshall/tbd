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
import tbd.visualization.analysis._
import tbd.ddg.{Tag, FunctionTag}
import scala.collection.mutable.{HashMap, ListBuffer}
import swing._
import swing.event._
import java.awt.{Color, Graphics2D, BasicStroke}

object DdgRenderer {
  def getFreeVarColorKey(index: Int): Color = {
    val colors = List(new Color(0, 0, 255),
                      new Color(0, 255, 0),
                      new Color(255, 0, 0),
                      new Color(255, 255, 0),
                      new Color(0, 255, 255),
                      new Color(255, 0, 255))

    colors(index % colors.length)
  }
}

class DdgRenderer extends Panel with Publisher {

  val CommandEdgeColor = Color.BLACK
  val RwEdgeColor = new Color(255, 255, 255, 0)
  val HighlightedRwEdgeColor = new Color(255, 0, 0)
  val SecondaryHighlightedRwEdgeColor = new Color(255, 64, 64)

  val vSpacing = 50
  val hSpacing = 50

  var translateX: Float = 0
  var translateY: Float = 0

  var scale: Float = 0.2f

  var lx = -1
  var ly = -1

  private var selectedNode: Node = null

  private val pos = new HashMap[Node, (Int, Int)]()
  private val nodes = new ListBuffer[Node]()
  private val controlEdges = new ListBuffer[(Node, Node)]()
  private var rwEdges: Iterable[(Node, Node)] = Seq()

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
    controlEdges.clear()
    nodes.clear()
    pos.clear()
    selectedNode = null
  }

  private def transform(pos: (Int, Int)): (Int, Int) = {
    val (x, y) = pos

    (((x + translateX) * scale).toInt, ((y + translateY) * scale).toInt)
  }

  private def inverseTransform(pos: (Int, Int)): (Int, Int) = {
    val (x, y) = pos

    ((x / scale - translateX).toInt, (y / scale - translateY).toInt)
  }

  override def paintComponent(g: Graphics2D) = {

    g.setColor(Color.WHITE)
    g.fillRect(0, 0, g.getClipBounds().width , g.getClipBounds().height)

    for(edge <- controlEdges) {
      val (x1, y1) = transform(getPos(edge._1))
      val (x2, y2) = transform(getPos(edge._2))

      g.setColor(CommandEdgeColor)
      g.drawLine(x1, y1, x2, y2)
    }

    if(selectedNode != null) {

      val rwEdges = ddg.adj(selectedNode).filter(x => x.isInstanceOf[Edge.ReadWrite])

      for(edge <- rwEdges) {
        val (x1, y1) = transform(getPos(edge.source))
        val (x2, y2) = transform(getPos(edge.destination))

        g.setColor(Color.RED)
        drawArrow(x1, y1, x2, y2, g)
      }

      val wrEdges = ddg.adj(selectedNode).filter(x => x.isInstanceOf[Edge.WriteRead])

      for(edge <- wrEdges) {
        val (x1, y1) = transform(getPos(edge.destination))
        val (x2, y2) = transform(getPos(edge.source))

        g.setColor(Color.RED)
        drawArrow(x1, y1, x2, y2, g)
      }

      val wmEdges = ddg.adj(selectedNode).filter(x => x.isInstanceOf[Edge.WriteMod])

      for(edge <- wmEdges) {
        val (x1, y1) = transform(getPos(edge.destination))
        val (x2, y2) = transform(getPos(edge.source))

        g.setColor(Color.RED)
        drawArrow(x1, y1, x2, y2, g)
      }

      val mwEdges = ddg.adj(selectedNode).filter(x => x.isInstanceOf[Edge.ModWrite])

      for(edge <- mwEdges) {
        val (x1, y1) = transform(getPos(edge.source))
        val (x2, y2) = transform(getPos(edge.destination))

        g.setColor(Color.RED)
        drawArrow(x1, y1, x2, y2, g)
      }

      val freeVarDeps = ddg.adj(selectedNode).filter(x => x.isInstanceOf[Edge.FreeVar])

      val stroke = g.getStroke()
      g.setStroke(new BasicStroke(2.0f,
                      BasicStroke.CAP_BUTT,
                      BasicStroke.JOIN_MITER,
                      10.0f, Array(10.0f), 0.0f))

      for(edge <- freeVarDeps.zipWithIndex) {
        val (x1, y1) = transform(getPos(edge._1.source))
        val (x2, y2) = transform(getPos(edge._1.destination))

        g.setColor(DdgRenderer.getFreeVarColorKey(edge._2))
        drawArrow(x1, y1, x2, y2, g)
      }

      g.setStroke(stroke)
    }

    for(node <- nodes) {
      val (x, y) = transform(getPos(node))

      val bg = getNodeBackgroundColor(node)

      node.tag match {
        case q:Tag.Read => drawRead(x, y, g, bg)
        case q:Tag.Write => drawWrite(x, y, g, bg)
        case q:Tag.Memo => drawMemo(x, y, g, bg)
        case q:Tag.Par => drawPar(x, y, g, bg)
        case q:Tag.Root => drawRoot(x, y, g, bg)
        case q:Tag.Mod => drawMod(x, y, g, bg)
      }
    }
  }

  def drawArrow(x1: Float, y1: Float, x2: Float, y2: Float, g: Graphics2D) = {

    val headSize = 5

    val transform = g.getTransform()
    val dx = x2 - x1
    val dy = y2 - y1
    val angle: Float = Math.atan2(dy, dx).toFloat
    var len: Int = Math.sqrt(dx * dx + dy * dy).toInt
    g.translate(x1, y1)
    g.rotate(angle)

    g.drawLine(0, 0, len, 0);
    len -= 4
    g.fillPolygon(Array[Int](len, len - headSize, len - headSize, len),
                  Array[Int](0, -headSize, headSize, 0), 4);

    g.setTransform(transform)
  }

  private def tracePath(
      src: Node,
      f: Edge => Boolean,
      color: Color,
      secondaryColor: Color,
      g: Graphics2D,
      reverseEdges: Boolean = false,
      traverseCallDependencies: Boolean = false) {
    for(edge <- ddg.adj(src).filter(f)) {
      val (x1, y1) = transform(getPos(edge.source))
      val (x2, y2) = transform(getPos(edge.destination))

      g.setColor(color)
      if(reverseEdges) {
        drawArrow(x1, y1, x2, y2, g)
      }
      else {
        drawArrow(x2, y2, x1, y1, g)
      }
      tracePath(edge.destination, f, color, secondaryColor, g,
                reverseEdges, traverseCallDependencies)
    }
    if(traverseCallDependencies) {
      for(edge <- ddg.adj(src).filter(x => x.isInstanceOf[Edge.Control])) {
        tracePath(edge.destination, f, secondaryColor, secondaryColor,
                  g, reverseEdges, traverseCallDependencies)
      }
    }
  }

  private def getNodeBackgroundColor(node: Node): Color = {
    if(comparison != null) {
      if(comparison.removed.contains(node)) {
        Color.RED
      } else if(comparison.added.contains(node)) {
        Color.YELLOW
      } else {
        null
      }
    } else {
      null
    }
  }

  val nodeSize = 5
  val backSize = 7

  private def drawRead(x: Int, y: Int, g: Graphics2D, bg: Color) {
    if(bg != null) {
      drawRoundNode(x, y, backSize, bg, g)
    }
    drawRoundNode(x, y, nodeSize, Color.BLUE, g)
  }

  private def drawWrite(x: Int, y: Int, g: Graphics2D, bg: Color) {
    if(bg != null) {
      drawRoundNode(x, y, backSize, bg, g)
    }
    drawRoundNode(x, y, nodeSize, Color.ORANGE, g)
  }

  private def drawMod(x: Int, y: Int, g: Graphics2D, bg: Color) {
    if(bg != null) {
      drawRoundNode(x, y, backSize, bg, g)
    }
    drawRoundNode(x, y, nodeSize, Color.MAGENTA, g)
  }

  private def drawRoot(x: Int, y: Int, g: Graphics2D, bg: Color) {
    if(bg != null) {
      drawRectNode(x, y, backSize, bg, g)
    }
    drawRectNode(x, y, nodeSize, Color.GRAY, g)
  }

  private def drawMemo(x: Int, y: Int, g: Graphics2D, bg: Color) {
    if(bg != null) {
      drawRectNode(x, y, backSize, bg, g)
    }
    drawRectNode(x, y, nodeSize, Color.GREEN, g)
  }

  private def drawPar(x: Int, y: Int, g: Graphics2D, bg: Color) {
    if(bg != null) {
      drawRectNode(x, y, backSize, bg, g)
    }
    drawRectNode(x, y, nodeSize, Color.YELLOW, g)
  }

  private def drawRoundNode(x: Int, y: Int, radius: Int, c: Color, g: Graphics2D) {
    g.setColor(c)
    g.fillOval(x - radius, y - radius, radius * 2, radius * 2)
  }

  private def drawRectNode(x: Int, y: Int, size: Int, c: Color, g: Graphics2D) {
    g.setColor(c)
    g.fillRect(x - size, y - size, size * 2, size * 2)
  }

  private def drawDIamondNode(x: Int, y: Int, size: Int, c: Color, g: Graphics2D) {
    val transform = g.getTransform()
    g.translate(x, y)
    g.rotate(Math.PI / 4)
    g.setColor(c)
    g.fillRect(-size, -size, size * 2, size * 2)
    g.setTransform(transform)
  }

  reactions += {
    case KeyPressed(_, key, _, _) => {
      var shallRepaint = true
      key match {
        case Key.Up => translateY -= 20 / scale
        case Key.Down => translateY += 20 / scale
        case Key.Left => translateX -= 20 / scale
        case Key.Right => translateX += 20 / scale
        case Key.PageUp => {
          zoom(size.width / 2, size.height / 2, 1)
        }
        case Key.PageDown => {
          zoom(size.width / 2, size.height / 2, -1)
        }
        case _ => shallRepaint = false
      }
      if(shallRepaint) {
    invokePerspectiveChanged()
        repaint()
      }
    }
    case MouseClicked(_, p, _, _, _) => {
      val (x, y) = inverseTransform((p.x, p.y))
      val selectedNode = nodes.map(n => {
        val (nx, ny) = pos(n)
        val dx = (nx - x)
        val dy = (ny - y)

        (dx * dx + dy * dy, n)
      }).sortWith((a, b) => a._1 < b._1).headOption

      if(!selectedNode.isEmpty) {
        this.selectedNode = selectedNode.get._2
        publish(NodeClickedEvent(selectedNode.get._2))
        repaint()
      }
    }
    case MouseDragged(_, point, _) => {
        if(lx != -1 && ly != -1) {
          translateX -= (lx - point.x) / scale
          translateY -= (ly - point.y) / scale
        }
        lx = point.x
        ly = point.y

        invokePerspectiveChanged()
        repaint()
    }
    case e: MouseReleased => {
      lx = -1
      ly = -1
    }
    case MouseWheelMoved(_, pt, _, dir) => {

      zoom(pt.x, pt.y, dir)
    }
    case _ => null
  }

  private def zoom(cx: Int, cy: Int, dir: Int) {
    val scaleSpeed = 1.3f
    val (x, y) = inverseTransform((cx, cy))

    if(dir < 0) {
      scale *= scaleSpeed
    } else {
      scale /= scaleSpeed
    }

    val (x2, y2) = inverseTransform((cx, cy))

    translateX -= (x - x2)
    translateY -= (y - y2)

    invokePerspectiveChanged()
    repaint()
  }

  private def invokePerspectiveChanged() {
    publish(PerspectiveChangedEvent(translateX, translateY, scale))
  }

  def setNewPerspective(translateX: Float, translateY: Float, scale: Float) {
    if(this.translateX != translateX ||
       this.translateY != translateY ||
       this.scale != scale) {

      this.translateX = translateX
      this.translateY = translateY
      this.scale = scale

      repaint()
    }
  }

  private def addNode(node: Node) = {
    nodes += node
  }

  private def removeNode(node: Node) {
    nodes -= node
  }

  private def addEdge(a: Node, b: Node) = {
    controlEdges += Tuple2(a, b)
  }

  private def removeEdge(a: Node, b: Node) {
    controlEdges -= Tuple2(a, b)
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

    ddg.getCallChildren(parent).foreach(child => {
      translateTree(child, dx)
    })
  }

  var ddg: DDG = null
  var comparison: ComparisonResult = null

  def showDDG(ddg: DDG, comparison: ComparisonResult = null) = {

    clear()

    val root = ddg.root
    this.ddg = ddg
    this.comparison = comparison

    createTree(root, null)
    layoutTree(root, 0)

    rwEdges = ddg.adj.flatMap(x => {
        x._2.filter(y => y.isInstanceOf[Edge.ReadWrite])
    }).map(x => (x.source, x.destination))

    this.repaint()
  }
}

case class NodeClickedEvent(val node: Node) extends event.Event
case class PerspectiveChangedEvent(val translateX: Float, val translateY: Float, val zoom: Float) extends event.Event