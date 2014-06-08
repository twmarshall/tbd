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
  val chunks = ArrayBuffer[String]()

  val rand = new scala.util.Random()
  val maxKey = count * 10
  def addValue(mutator: Mutator, table: Map[Int, String]) {
    if (chunks.size == 0) {
      chunks ++= Experiment.loadPages()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)

    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }

    println("addValue " + key + " -> " + value)

    mutator.put(key, chunks.head)
    table += (key -> chunks.head)
    chunks -= chunks.head
  }

  def removeValue(mutator: Mutator, table: Map[Int, String]) {
    if (table.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }
      println("removeValue " + key)
      mutator.remove(key)
      table -= key
    } else {
      addValue(mutator, table)
    }
  }

  def updateValue(mutator: Mutator, table: Map[Int, String]) {
    if (chunks.size == 0) {
      chunks ++= Experiment.loadPages()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
      println("updateValue " + key + " -> " + value)
    mutator.update(key, chunks.head)
    table(key) = chunks.head
    chunks -= chunks.head
  }

  def update(mutator: Mutator, table: Map[Int, String]) {
    mutations(rand.nextInt(mutations.size)) match {
      case "insert" => addValue(mutator, table)
      case "remove" => removeValue(mutator, table)
      case "update" => updateValue(mutator, table)
    }
  }

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
      case "map" => new MapAdjust(partition, false, false, false)
      case "pmap" => new MapAdjust(partition, false, true, false)
      case "mmap" => new MapAdjust(partition, false, false, true)
      case "mpmap" => new MapAdjust(partition, false, true, true)

      case "dmap" => new MapAdjust(partition, true, false, false)
      case "dpmap" => new MapAdjust(partition, true, true, false)
      case "dmmap" => new MapAdjust(partition, true, false, true)
      case "dmpmap" => new MapAdjust(partition, true, true, true)

      case "cmap" => new ChunkMapAdjust(partition, chunkSize, false, false, false)
      case "cpmap" => new ChunkMapAdjust(partition, chunkSize, false, true, false)
      case "cmmap" => new ChunkMapAdjust(partition, chunkSize, false, false, true)
      case "cmpmap" => new ChunkMapAdjust(partition, chunkSize, false, true, true)

      case "dcmap" => new ChunkMapAdjust(partition, chunkSize, true, false, false)
      case "dcpmap" => new ChunkMapAdjust(partition, chunkSize, true, true, false)
      case "dcmmap" => new ChunkMapAdjust(partition, chunkSize, true, false, true)
      case "dcmpmap" => new ChunkMapAdjust(partition, chunkSize, true, true, true)
      // Filter
      case "filter" => new FilterAdjust(partition, false, false)
      case "pfilter" => new FilterAdjust(partition, true, false)
      case "mfilter" => new FilterAdjust(partition, false, true)
      case "mpfilter" => new FilterAdjust(partition, true, true)
      // Wordcount
      case "wc" => new WCAdjust(partition, false, false)
      case "pwc" => new WCAdjust(partition, false, true)
      case "dwc" => new WCAdjust(partition, true, false)
      case "dpwc" => new WCAdjust(partition, true, true)

      case "cwc" => new ChunkWCAdjust(partition, chunkSize, false, false)
      case "cpwc" => new ChunkWCAdjust(partition, chunkSize, false, true)
      case "dcwc" => new ChunkWCAdjust(partition, chunkSize, true, false)
      case "dcpwc" => new ChunkWCAdjust(partition, chunkSize, true, true)

      // Split
      case "split" => new SplitAdjust(partition, false, false)
      case "psplit" => new SplitAdjust(partition, true, false)
      case "msplit" => new SplitAdjust(partition, false, true)
      case "mpsplit" => new SplitAdjust(partition, true, true)
    }

    val tableForTraditionalRun = alg.prepareTraditionalRun(table)

    val beforeTraditional = System.currentTimeMillis()
    alg.traditionalRun(tableForTraditionalRun)
    val traditionalElapsed = System.currentTimeMillis() - beforeTraditional

    results("nontbd") = traditionalElapsed

    for (pair <- table) {
      mutator.put(pair._1, pair._2)

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
	  input.update(mutator, table)
        }

        val before2 = System.currentTimeMillis()
        mutator.propagate()
        val elapsed = System.currentTimeMillis() - before2

	if (Experiment.check) {
          if(!alg.checkOutput(table)){
            val visualizer = new tbd.visualization.TbdVisualizer()
            visualizer.showLabels = false
            visualizer.showDDG(mutator.getDDG().root)
            assert(false)
          }
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
