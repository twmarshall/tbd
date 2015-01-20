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

package thomasdb.visualization

import scala.collection.mutable.{Map}
import scala.math.{min, max}
import thomasdb.visualization.analysis._

/**
 * Generates a plot whereas the x-axis is the starting position of a mutation,
 * the y-axis is the length of a mutation, and the values are the average trace
 * distance.
 *
 * This plot is basically only meaningful for continous updates.
 */
class UpdateLengthPositionPlot[T](distanceAlgorithm: TraceComparison)
  extends TraceDistancePlotGenerator[T](distanceAlgorithm) {

  var maxLen = 0
  var maxPos = 0

  //Aggregates the update data and generates the plot.
  def getPlot(): PlotInfo = {

    //Maps (length, start position) to (distance)
    val sparseData = Map[(Int, Int), Int]()

    var lastExperiment = experiments.head

    for(experiment <- experiments.tail) {

      //Calculate update start position and update length for this experiment.
      var startPos = Integer.MAX_VALUE
      var length = experiment.mutations.size

      //We only allow updates here.
      for(update <- experiment.mutations){
        update match {
          case Update(k, n, o) => startPos = min(k, startPos)
          case _ => throw new IllegalArgumentException("Only update mutations allowed!")
        }
      }

      //For each experiment, compute the trace distance to the last experiment.
      val comparison = distanceAlgorithm.compare(lastExperiment.ddg, experiment.ddg)

      sparseData((length, startPos)) = comparison.distance

      //Remember the maximum occouring length and position, so we know how long
      //our axis are.
      maxLen = max(maxLen, length)
      maxPos = max(maxPos, startPos)

      lastExperiment = experiment
    }

    //Create data structures holding the data.
    var data = new Array[Array[Float]](maxLen)
    val xaxis = new Array[Float](maxLen)
    val yaxis = new Array[Float](maxPos)
    var count = new Array[Array[Int]](maxLen)
    for(i <- 0 to maxLen - 1) {
      data(i) = new Array[Float](maxPos)
      count(i) = new Array[Int](maxPos)
      xaxis(i) = i + 1
    }

    for(i <- 0 to maxPos - 1) {
      yaxis(i) = i + 1
    }

    //Convert the sparse data map to the output array.
    for(d <- sparseData) {
      data(d._1._1 - 1)(d._1._2 - 1) += d._2
      count(d._1._1 - 1)(d._1._2 - 1) += 1
    }

    //Calculate the average of all corresponding data points.
    for(i <- 0 to maxLen - 1)
      for(j <- 0 to maxPos - 1)
        if(count(i)(j) != 0)
          data(i)(j) = data(i)(j) / count(i)(j)

    PlotInfo(data, xaxis, yaxis, "Starting Position",
             "Update Length", "Update Trace Distance");
  }
}
