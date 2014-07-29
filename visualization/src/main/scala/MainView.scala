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

  val label = new TextArea("To calculate trace distance, select two different DDGs.")
  label.editable = false
  label.background = java.awt.Color.LIGHT_GRAY

  def addResult(result: ExperimentResult[Any, Any]) {
    visualizer1.addResult(result)
    visualizer2.addResult(result)

    listenTo(visualizer1)
    listenTo(visualizer2)
  }

  reactions += {
    case SelectedDDGChanged(ddg) => {
      println("selection changed")
      updateDiff()
    }
  }

  private def updateDiff() {
    if(visualizer1.ddg != null && visualizer2.ddg != null) {
      val diff = TraceComparison.greedyTraceDistance(visualizer1.ddg.ddg, visualizer2.ddg.ddg, (node => node.tag))
      visualizer1.setComparisonResult(diff)
      visualizer2.setComparisonResult(diff)

      val tbdDiff = TraceComparison.greedyTraceDistance(visualizer1.ddg.ddg, visualizer2.ddg.ddg, (node => node.internalId))

      label.text = "Trace Distance: " + (diff.added.length + diff.removed.length) + "\n" +
      "TBD Distance: " + (tbdDiff.added.length + tbdDiff.removed.length)
    }
  }


  val visualizer1 = new DdgVisualizer()
  val visualizer2 = new DdgVisualizer()
  contents = new GridBagPanel() {
    layout(new SplitPane(Orientation.Vertical) {
      contents_$eq(visualizer1, visualizer2)
    }) = new Constraints() {
      gridx = 0
      gridy = 0
      weighty = 1
      weightx = 1
      fill = GridBagPanel.Fill.Both
    }
    layout(label) = new Constraints() {
      gridx = 0
      gridy = 1
      fill = GridBagPanel.Fill.Horizontal
    }
  }
  pack()

  visible = true
}