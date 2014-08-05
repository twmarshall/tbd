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

import graph._
import analysis._
import scala.swing._
import scala.swing.event._
import tbd.ddg.{Tag, FunctionTag}

object QuickVisualizer {
  def create() = {
    val view = new MainView(false)
    view.visualizer1
  }
  def show(ddg: tbd.ddg.DDG) {
    val view = new MainView(false)
    view.addResult(ExperimentResult(0,
        List(), List(), List(), graph.DDG.create(ddg.root)))
  }
}

class DdgVisualizer extends GridBagPanel with Publisher {
  val renderer = new DdgRenderer()
  listenTo(renderer)
  var ddg: ExperimentResult[Any, Any] = null
  var comboBoxItems = List[ExperimentResult[Any, Any]]()
  var selector: ComboBox[ExperimentResult[Any, Any]] = null

  val htmlInto = "<html><body style=\"font-family: monospaced\">"
  val htmlOutro = "</body></html>"
  val label = new javax.swing.JTextPane() {
    setContentType("text/html")
    setEditable(false)
    setBackground(java.awt.Color.LIGHT_GRAY)
  }
  setLabelText("Click node for info.<br />Scroll by dragging the mouse.<br />" +
               "Zoom with scroll wheel.")
  var selectedNode: Node = null

  def setLabelText(text: String) {
    label.setText(htmlInto + text + htmlOutro)
  }

  def htmlEscape(text: String): String = {
    text.
    replaceAll("  ", "&nbsp;&nbsp;").
    replaceAll("<", "&lt;").
    replaceAll(">", "&gt;").
    replaceAll("\n", "<br />")
  }

  def toHtmlString(o: Any): String = {
    if(o == null) {
      "null"
    } else {
      htmlEscape(o.toString())
    }
  }

  def initComboBox() = {
    if(selector != null) {
      layout -= selector
      deafTo(selector.selection)
    }

    selector = new ComboBox(comboBoxItems) {
      renderer = ListView.Renderer(x =>
        if(x == null) {
          "None"
        } else {
          (x.runId + " - " + x.input)
        }
      )
    }

    listenTo(selector.selection)

    if(comboBoxItems.length == 1) {
      selector.selection.item = comboBoxItems(0)
    }

    layout(selector) = new Constraints() {
      gridx = 0
      gridy = 0
      fill = GridBagPanel.Fill.Horizontal
    }

    this.revalidate()

  }

  def formatFunctionTag(node: Node, tag: FunctionTag): String = {
    val freeVarEdges = ddg.ddg.adj(node).filter({
        x => x.isInstanceOf[Edge.FreeVar]
    }).map(x => x.asInstanceOf[Edge.FreeVar])

    val freeVarsText = freeVarEdges.zipWithIndex.flatMap(edge => {
      val color = DdgRenderer.getFreeVarColorKey(edge._2)
      val colorString = "rgb(" + color.getRed() + ", " + color.getGreen() +
                        ", " + color.getBlue() + ")"

      edge._1.dependencies.map(dep => {
        "<font color=\"" + colorString + "\">" +
        toHtmlString(dep._1) + " = " + toHtmlString(dep._2) +
        "</font>"
      })
    }).foldLeft("")(_ + "<br />&nbsp;&nbsp;&nbsp;" + _)

    val varsFromOutsideScopes =
    tag.freeVars.filter(dep => {
      !freeVarEdges.flatMap(x => x.dependencies).contains(dep)
    }).map(dep => {
      toHtmlString(dep._1) + " = " + toHtmlString(dep._2)
    }).foldLeft("")(_ + "<br />&nbsp;&nbsp;&nbsp;" + _)

    "Function: " + tag.funcId +
    "<br />Bound free vars:" + freeVarsText +
    "<br />Free vars from outside scopes:" + varsFromOutsideScopes
  }

  def formatTag(node: Node): String = {
    node.tag match{
      case read @ Tag.Read(value, funcTag) => {
          "Read " + read.mod + " = " + toHtmlString(value) +
          "<br>Reader " + formatFunctionTag(node, funcTag)
      }
      case Tag.Write(writes) => {
        writes.map(x =>  {
          "write " + x.value + " to " + x.mod
        }).reduceLeft(_ + "<br />   " + _)
      }
      case Tag.Memo(funcTag, signature) => {
          "Memo" +
          "<br>" + formatFunctionTag(node, funcTag) +
          "<br>Signature:" + htmlEscape(signature.foldLeft("")(_ + "\n   " + _))
      }
      case Tag.Mod(dests, initializer) => {
          "Mod id " + dests.reduceLeft(_ + ", " + _) +
          "<br>Initializer " + formatFunctionTag(node, initializer)
      }
      case Tag.Par(fun1, fun2) => {
          "Par" +
          "<br>First " + formatFunctionTag(node, fun1) +
          "<br>Second " + formatFunctionTag(node, fun2)
      }
      case Tag.Root() => "(Root)"
    }
  }

  initComboBox()

  reactions += {
    case NodeClickedEvent(node) => {
      selectedNode = node
      setLabelText(htmlEscape(extractMethodName(node)) +
                   "<br />" + formatTag(node))
    }
    case x:PerspectiveChangedEvent => {
      //publish(x)
    }
    case x:SelectionChanged => {
      ddg = selector.selection.item
      renderer.showDDG(ddg.ddg)
      publish(SelectedDDGChanged(ddg.ddg))
    }
  }

  def addResult(result: ExperimentResult[Any, Any]) {
    comboBoxItems =  comboBoxItems :+ result
    initComboBox()
  }

  def setComparisonResult(diff: ComparisonResult) {
      renderer.showDDG(ddg.ddg, diff)
  }

  val scrollPane = new ScrollPane(new Component() {
    override lazy val peer = label
  })

  val splitPane = new SplitPane(Orientation.Horizontal) {
    contents_$eq(renderer, scrollPane)
  }

  layout(splitPane) = new Constraints() {
    gridx = 0
    gridy = 1
    weighty = 1
    weightx = 1
    fill = GridBagPanel.Fill.Both
  }

  private def extractMethodName(node: Node): String = {

    if(node.stacktrace == null) {
      return "<No stacktrace available. Set Main.debug = true to enable " +
             "stacktraces>"
    }

    val methodNames = node.stacktrace.map(y => y.getMethodName())
    val fileNames = node.stacktrace.map(y => {
      (y.getMethodName(),y.getFileName(), y.getLineNumber())
    })

    var (_, fileName, lineNumber) = fileNames.filter(y => {
        y._1.contains("apply")
    })(0)

    val currentMethodOption = methodNames.filter(y => (!y.startsWith("<init>")
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
                                            && !y.startsWith("mod"))).headOption

    if(!currentMethodOption.isEmpty) {
      var currentMethod = currentMethodOption.get

      if(currentMethod.contains("$")) {
        currentMethod = currentMethod.substring(0, currentMethod.lastIndexOf("$"))
        currentMethod = currentMethod.substring(currentMethod.lastIndexOf("$") + 1)
      }

      if(methodNames.find(x => x == "createMod").isDefined) {
        currentMethod += " (createMod)"
      }

      "Method " + currentMethod + " at " + fileName + ":" + lineNumber.toString
    } else {
      "<unknown>"
    }
  }
}

case class SelectedDDGChanged(ddg: DDG) extends Event