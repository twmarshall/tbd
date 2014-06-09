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
package tbd.examples.list

import java.io.{BufferedInputStream, File, FileInputStream}
import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.Mutator

class AdjustableExperiment(aConf: Map[String, _])
    extends Experiment(aConf) {
  val input = new WCInput(count * 10, mutations)

  def run(): Map[String, Double] = {
    val results = Map[String, Double]()

    val mutator = new Mutator()
    val table = Map[Int, String]()

    var i = 0
    while (table.size < count) {
      if (input.chunks.size == 0) {
        input.chunks ++= Experiment.loadPages()
      }

      table += (i -> input.chunks.head)
      input.chunks -= input.chunks.head
      i += 1
    }

    // prefix order: double, chunk, memoized, parallel algorithm
    val alg = algorithm match {
      // Map
      case "map" => new MapAdjust(mutator, partition, false, false, false)
      case "pmap" => new MapAdjust(mutator, partition, false, true, false)
      case "mmap" => new MapAdjust(mutator, partition, false, false, true)
      case "mpmap" => new MapAdjust(mutator, partition, false, true, true)

      case "dmap" => new MapAdjust(mutator, partition, true, false, false)
      case "dpmap" => new MapAdjust(mutator, partition, true, true, false)
      case "dmmap" => new MapAdjust(mutator, partition, true, false, true)
      case "dmpmap" => new MapAdjust(mutator, partition, true, true, true)

      case "cmap" => new ChunkMapAdjust(mutator, partition, chunkSize, false, false, false)
      case "cpmap" => new ChunkMapAdjust(mutator, partition, chunkSize, false, true, false)
      case "cmmap" => new ChunkMapAdjust(mutator, partition, chunkSize, false, false, true)
      case "cmpmap" => new ChunkMapAdjust(mutator, partition, chunkSize, false, true, true)

      case "dcmap" => new ChunkMapAdjust(mutator, partition, chunkSize, true, false, false)
      case "dcpmap" => new ChunkMapAdjust(mutator, partition, chunkSize, true, true, false)
      case "dcmmap" => new ChunkMapAdjust(mutator, partition, chunkSize, true, false, true)
      case "dcmpmap" => new ChunkMapAdjust(mutator, partition, chunkSize, true, true, true)
      // Filter
      case "filter" => new FilterAdjust(mutator, partition, false, false)
      case "pfilter" => new FilterAdjust(mutator, partition, true, false)
      case "mfilter" => new FilterAdjust(mutator, partition, false, true)
      case "mpfilter" => new FilterAdjust(mutator, partition, true, true)
      // Wordcount
      case "wc" => new WCAdjust(mutator, partition, false, false)
      case "pwc" => new WCAdjust(mutator, partition, false, true)
      case "dwc" => new WCAdjust(mutator, partition, true, false)
      case "dpwc" => new WCAdjust(mutator, partition, true, true)

      case "cwc" => new ChunkWCAdjust(mutator, partition, chunkSize, false, false)
      case "cpwc" => new ChunkWCAdjust(mutator, partition, chunkSize, false, true)
      case "dcwc" => new ChunkWCAdjust(mutator, partition, chunkSize, true, false)
      case "dcpwc" => new ChunkWCAdjust(mutator, partition, chunkSize, true, true)
    }

    val tableForTraditionalRun = alg.prepareTraditionalRun(table)

    val beforeTraditional = System.currentTimeMillis()
    alg.traditionalRun(tableForTraditionalRun)
    val traditionalElapsed = System.currentTimeMillis() - beforeTraditional

    results("nontbd") = traditionalElapsed

    for (pair <- table) {
      alg.input.put(pair._1, pair._2)

      if (!Experiment.check) {
        table(pair._1) = ""
      }
    }

    val beforeInitial = System.currentTimeMillis()
    alg.initialRun(mutator)
    val initialElapsed = System.currentTimeMillis() - beforeInitial

    if (Experiment.check) {
      assert(alg.checkOutput(table))
    }

    results("initial") = initialElapsed

    if (Experiment.verbose) {
      println("map count = " + alg.mapCount)
      println("reduce count = " + alg.reduceCount)
      println("starting prop")
      alg.mapCount = 0
      alg.reduceCount = 0
    }

    for (percent <- percents) {
      if (percent != "initial" && percent != "nontbd") {
        var i =  0

	val updateCount =
	  if (percent.toDouble < 1)
	    percent.toDouble * count
	  else
	    percent.toDouble

        while (i < updateCount) {
	  i += 1
	  input.update(alg.input, table)
        }

        val before2 = System.currentTimeMillis()
        mutator.propagate()
        val elapsed = System.currentTimeMillis() - before2

	if (Experiment.check) {
          assert(alg.checkOutput(table))
	}

        results(percent) = elapsed
      }
    }
    if (Experiment.verbose) {
      println("map count = " + alg.mapCount)
      println("reduce count = " + alg.reduceCount)
    }
    mutator.shutdown()

    results
  }
}
