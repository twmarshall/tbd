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
import tbd.ddg.{Tag, FunctionTag}

class MainView extends MainFrame {

  val visualizer1 = new TbdVisualizer()
  val visualizer2 = new TbdVisualizer()

  listenTo(visualizer1)
  listenTo(visualizer2)

  var ddg1: DDG = null
  var ddg2: DDG = null

  reactions += {
    case event.ButtonClicked(button) => {
      println("button clicked.")
      if(buttonGroupA.buttons.contains(button)) {
        var res = buttonTags(button.asInstanceOf[RadioButton])
        if(res != null) {
          ddg1 = res.ddg
          updateDDGDisplay()
        }
      } else if(buttonGroupB.buttons.contains(button)) {
        var res = buttonTags(button.asInstanceOf[RadioButton])
        if(res != null) {
          ddg2 = res.ddg
          updateDDGDisplay()
        }
      }
    }
    case NodeClickedEvent(node) => {
      label.text = formatTag(node.tag)
    }
    case PerspectiveChangedEvent(dx, dy, s) => {
      visualizer1.setNewPerspective(dx, dy, s)
      visualizer2.setNewPerspective(dx, dy, s)
    }
  }

  private def updateDDGDisplay() {

    val diff = compareDDGs()

    if(ddg1 != null)
      visualizer1.showDDG(ddg1, diff)
    if(ddg2 != null)
      visualizer2.showDDG(ddg2, diff)
  }

  private def compareDDGs(): ComparisonResult = {
    if(ddg1 != null && ddg2 != null) {
      TraceComparison.greedyTraceDistance(ddg1, ddg2)
    } else {
      null
    }
  }

  var label = new TextArea("Click node for info.\nScroll with arrow keys.\nZoom with PgDown and PgUp.")
  label.editable = false
  label.background = java.awt.Color.LIGHT_GRAY

  val buttonGroupA = new ButtonGroup()
  val buttonGroupB = new ButtonGroup()

  def addResult(result: ExperimentResult[Any, Any]) {
    val selector = new SelectionView(result, buttonGroupA, buttonGroupB)
    this.listenTo(selector.select1)
    this.listenTo(selector.select2)

    box.contents += selector
    pack()
  }

  val buttonTags = new scala.collection.mutable.HashMap[RadioButton, ExperimentResult[Any, Any]]

  class SelectionView(result: ExperimentResult[Any, Any], groupA: ButtonGroup, groupB: ButtonGroup) extends GridBagPanel {
    layout(new Label(result.runId + " - " + result.input, null, Alignment.Left))  = new Constraints() {
      gridx = 0
      gridy = 0
      weighty = 1
      weightx = 1
      fill = GridBagPanel.Fill.Both
    }

    val select1 = new RadioButton()
    layout(select1)  = new Constraints() {
      gridx = 1
      gridy = 0
      weighty = 1
      fill = GridBagPanel.Fill.Vertical
    }

    val select2 = new RadioButton()
    layout(select2)  = new Constraints() {
      gridx = 2
      gridy = 0
      weighty = 1
      fill = GridBagPanel.Fill.Vertical
    }

    groupA.buttons += select1
    groupB.buttons += select2

    buttonTags(select1) = result
    buttonTags(select2) = result

    this.maximumSize = new Dimension(9999999, 20)
  }

  val box = new BoxPanel(Orientation.Vertical)

  contents = new GridBagPanel(){
    layout(box)  = new Constraints() {
      gridx = 0
      gridy = 0
      weighty = 1
      fill = GridBagPanel.Fill.Vertical
    }
    layout(visualizer1)  = new Constraints() {
      gridx = 1
      gridy = 0
      weighty = 1
      weightx = 1
      fill = GridBagPanel.Fill.Both
    }
    layout(visualizer2)  = new Constraints() {
      gridx = 2
      gridy = 0
      weighty = 1
      weightx = 1
      fill = GridBagPanel.Fill.Both
    }
    layout(new ScrollPane(label))  = new Constraints() {
      gridx = 1
      gridy = 2
      weighty = 0.3
      weightx = 2
      gridwidth = 2
      fill = GridBagPanel.Fill.Both
    }
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
          "Mod id " + dest +
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

  visible = true
}