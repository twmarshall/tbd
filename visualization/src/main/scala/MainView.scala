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

class MainView(diffMode: Boolean) extends MainFrame {


  val label = if(diffMode) {
    new TextArea("To calculate trace distance, select two different DDGs.") {
      editable = false
      background = java.awt.Color.LIGHT_GRAY
    }
  } else {
    null
  }

  def addResult(result: ExperimentResult[Any, Any]) {
    visualizer1.addResult(result)
    if(diffMode) { visualizer2.addResult(result) }
  }

  reactions += {
    case SelectedDDGChanged(ddg) => {
      updateDiff()
    }
  }

  private def updateDiff() {
    if(visualizer1.ddg != null && diffMode && visualizer2.ddg != null) {
      val diff = TraceComparison.greedyTraceDistance(visualizer1.ddg.ddg,
                    visualizer2.ddg.ddg, (node => node.tag))
      visualizer1.setComparisonResult(diff)
      visualizer2.setComparisonResult(diff)

      val tbdDiff = TraceComparison.greedyTraceDistance(visualizer1.ddg.ddg,
                        visualizer2.ddg.ddg, (node => node.internalId))

      label.text = "Tree size left: " + visualizer1.ddg.ddg.nodes.size +
        ", right: " + visualizer2.ddg.ddg.nodes.size +
        "\nTrace Distance: " +
        (diff.added.length + diff.removed.length) +
        " (Added: " + diff.removed.length + ", removed: " + diff.added.length + ") \n" +
        "TBD Distance: " + (tbdDiff.added.length + tbdDiff.removed.length) +
        " (Added: " + tbdDiff.removed.length + ", removed: " + tbdDiff.added.length + ")"
    }
  }


  val visualizer1 = new DdgVisualizer()
  val visualizer2 = if(diffMode) { new DdgVisualizer() } else { null }

  listenTo(visualizer1)

  if(diffMode) {
    listenTo(visualizer2)
  }
  contents = new GridBagPanel() {
    layout(
      if(diffMode) {
        new SplitPane(Orientation.Vertical) {
          contents_$eq(visualizer1, visualizer2)
        }
      } else {
        visualizer1
      }
    ) = new Constraints() {
      gridx = 0
      gridy = 0
      weighty = 1
      weightx = 1
      fill = GridBagPanel.Fill.Both
    }
    if(diffMode) {
      layout(label) = new Constraints() {
        gridx = 0
        gridy = 1
        fill = GridBagPanel.Fill.Horizontal
      }
    }
  }
  pack()

  visible = true
}