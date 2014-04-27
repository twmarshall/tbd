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
package tbd.examples.wordcount

import java.io.{BufferedInputStream, File, FileInputStream}
import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.{Adjustable, Mutator}
import tbd.datastore.Dataset
import tbd.master.Main
import tbd.mod.Mod

class Experiment(conf: ExperimentConf) {
  val runtime = Runtime.getRuntime()
  val rand = new scala.util.Random()
  val wc = new WC()

  print("desc\tpages\tinitial")
  for (percent <- conf.percents) {
    print("\t" + (percent * 100) + "%")
  }
  print("\n")

  def warmUp() {
    var oldCounts = conf.counts
    conf.counts = Array(1000)
    run(new MapMemoParAdjust(8), "warmup")
    conf.counts = oldCounts
  }

  def round(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  val chunks = ArrayBuffer[String]()
  def loadFile() {
    val source = scala.io.Source.fromFile("input.txt")

    val bb = new Array[Byte](conf.chunkSize)
    val bis = new BufferedInputStream(new FileInputStream(new File("input.txt")))
    var bytesRead = bis.read(bb, 0, conf.chunkSize)

    while (bytesRead > 0) {
      chunks += new String(bb)
      bytesRead = bis.read(bb, 0, conf.chunkSize)
    }
  }

  var nextChunk = 0
  val activeChunks = ArrayBuffer[Int]()
  val activeChunkValues = Map[Int, String]()
  def addInput(start: Int, stop: Int, main: Main) {
    val mutator = new Mutator(main)

    var i = start
    while (i < stop) {
      putChunk(mutator)
      i += 1
    }

    mutator.shutdown()
  }

  def putChunk(mutator: Mutator) {
    mutator.put(nextChunk, chunks.head)
    activeChunks += nextChunk
    activeChunkValues += (nextChunk -> chunks.head)
    nextChunk += 1
    chunks -= chunks.head

    if (chunks.size == 0) {
      loadFile()
    }
  }

  def runControl(description: String, control: (Int, Int, Int) => Long) {
    allResults(description) = Map[Int, Map[String, Double]]()
    for (count <- conf.counts) {
      allResults(description)(count) = Map[String, Double]()
      val time = control(count, conf.repeat, conf.chunkSize)
      for (percent <- "initial" +: conf.percents) {
        allResults(description)(count)(percent+"") = time
      }
    }
  }

  def once(
      adjust: Adjustable,
      aCount: Int,
      results: Map[String, Double],
      main: Main) {
    val mutator = new Mutator(main)
    var count = aCount

    while (activeChunks.size < count) {
      putChunk(mutator)
    }

    val before = System.currentTimeMillis()
    val output = mutator.run[Dataset[Int]](adjust)
    val initialElapsed = System.currentTimeMillis() - before

    val sortedOutput = output.toBuffer().sortWith(_ < _)
    val sortedAnswer = activeChunkValues.map(pair => {
      MapAdjust.mapper(pair._2)
    }).toBuffer.sortWith(_ < _)
    assert(sortedOutput == sortedAnswer)

    if (results.contains("initial")) {
      results("initial") += initialElapsed
    } else {
      results("initial") = initialElapsed
    }
    print("\t" + initialElapsed)

    for (percent <- conf.percents) {
      var i =  0
      while (i < percent * count) {
        i += 1

        if (activeChunks.size == 1) {
          putChunk(mutator)
        } else {
          rand.nextInt(3) match {
            case 0 => {
              val updated = rand.nextInt(activeChunks.size)
              mutator.update(activeChunks(updated), chunks.head)
              activeChunkValues(activeChunks(updated)) = chunks.head
              chunks -= chunks.head
              if (chunks.size == 0) {
                loadFile()
              }
            }
            case 1 => {
              putChunk(mutator)
            }
            case 2 => {
              val removedChunkId = activeChunks(rand.nextInt(activeChunks.size))
              mutator.remove(removedChunkId)
              activeChunks -= removedChunkId
              activeChunkValues -= removedChunkId
            }
          }
        }
      }
      val before2 = System.currentTimeMillis()
      mutator.propagate()
      val elapsed = System.currentTimeMillis() - before2
      print("\t" + elapsed)

      val sortedOutput = output.toBuffer().sortWith(_ < _)
      val sortedAnswer = activeChunkValues.map(pair => {
        MapAdjust.mapper(pair._2)
      }).toBuffer.sortWith(_ < _)
      assert(sortedOutput == sortedAnswer)

      if (results.contains(percent + "")) {
        results(percent + "") += elapsed
      } else {
        results(percent + "") = elapsed
      }
    }

    println("")
    mutator.shutdown()
  }

  val allResults = Map[String, Map[Int, Map[String, Double]]]()
  def run(adjust: Adjustable, description: String) {
    val main = new Main()
    loadFile()

    val resultsByCount = Map[Int, Map[String, Double]]()
    for (count <- conf.counts) {
      var run = 0
      val results = Map[String, Double]()
      while (run < conf.repeat) {
        print(description + "\t" + count)
        once(adjust, count, results, main)
        run += 1
      }

      results("initial") = round(results("initial") / conf.repeat)

      for (percent <- conf.percents) {
        results(percent + "") = round(results(percent + "") / conf.repeat)
      }

      resultsByCount(count) = results
    }

    allResults(description) = resultsByCount
    main.shutdown()
    activeChunks.clear()
    activeChunkValues.clear()
  }

  def printFormattedResults() {
    for (percent <- "initial" +: conf.percents) {
      print(percent + "\t")
      for (description <- conf.descriptions) {
        print(description + "\t")
      }
      print("\n")

      for (count <- conf.counts) {
        print(count + "\t")

        for (description <- conf.descriptions) {
          print(allResults(description)(count)(percent + "") + "\t")
        }
        print("\n")
      }
    }
  }
}

object Experiment {
  val usage = """
    Usage: run.sh [--repeat num] [--counts int,int,...] [--percents float,float,...]
      [--chunkSize int] [--descriptions seq,par,memo,...] [--partitions int]
  """

  def main(args: Array[String]) {
    val conf = new ExperimentConf()

    def parse(list: List[String]) {
      list match {
        case Nil =>
        case "--repeat" :: value :: tail =>
          conf.repeat = value.toInt
          parse(tail)
        case "--chunkSize" :: value :: tail =>
          conf.chunkSize = value.toInt
          parse(tail)
        case "--counts" :: value :: tail =>
          println("found counts")
          conf.counts = value.split(",").map(_.toInt)
          parse(tail)
        case "--descriptions" :: value :: tail =>
          conf.descriptions = value.split(",")
          parse(tail)
        case "--percents" :: value :: tail =>
          conf.percents = value.split(",").map(_.toDouble)
          parse(tail)
        case "--partitions" :: value :: tail =>
          conf.partitions = value.toInt
          parse(tail)
        case "--algorithm" :: value :: tail =>
          conf.algorithm = value
          parse(tail)
        case option :: tail => println("Unknown option " + option + "\n" + usage)
      }
    }
    parse(args.toList)

    val experiment = new Experiment(conf)
    experiment.loadFile()
    experiment.warmUp()
    for (description <- conf.descriptions) {
      if (conf.algorithm == "map") {
        description match {
          case "smap" =>
            experiment.runControl("smap", SimpleMap.run)
          case "seq" =>
            experiment.run(new MapAdjust(conf.partitions), description)
          case "par" =>
            experiment.run(new MapParAdjust(conf.partitions), description)
          case "memo" =>
            experiment.run(new MapMemoAdjust(conf.partitions), description)
          case "memopar" =>
            experiment.run(new MapMemoParAdjust(conf.partitions), description)
          case _ =>
            println("Unrecognized description.")
        }
      } else {
        experiment.run(new WCAdjust(conf.partitions), "seq")
        experiment.run(new WCParAdjust(conf.partitions), "par")
      }
    }

    experiment.printFormattedResults()
  }
}
