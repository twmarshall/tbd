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

class DdgVisualizer extends GridBagPanel with Publisher {
  val renderer = new DdgRenderer()
  listenTo(renderer)
  var ddg: ExperimentResult[Any, Any] = null
  var comboBoxItems = List[ExperimentResult[Any, Any]]()
  var selector: ComboBox[ExperimentResult[Any, Any]] = null
  val label = new TextArea("Click node for info.\nScroll with arrow keys.\nZoom with PgDown and PgUp.")
  var selectedNode: Node = null
  label.editable = false
  label.background = java.awt.Color.LIGHT_GRAY

  def initComboBox() = {
    if(selector != null) {
      layout -= selector
      deafTo(selector.selection)
    }

    selector = new ComboBox(comboBoxItems) {
      renderer = ListView.Renderer(x => if(x == null) "None" else (x.runId + " - " + x.input))
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

  initComboBox()

  reactions += {
    case NodeClickedEvent(node) => {
      selectedNode = node
      label.text = "in " + extractMethodName(node) + ":\n" + tbd.ddg.Tag.formatTag(node.tag)
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

  layout(renderer)  = new Constraints() {
    gridx = 0
    gridy = 1
    weighty = 1
    weightx = 1
    fill = GridBagPanel.Fill.Both
  }

  layout(new ScrollPane(label))  = new Constraints() {
    gridx = 0
    gridy = 2
    weighty = 0.2
    weightx = 1
    fill = GridBagPanel.Fill.Both
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
}

case class SelectedDDGChanged(ddg: DDG) extends Event