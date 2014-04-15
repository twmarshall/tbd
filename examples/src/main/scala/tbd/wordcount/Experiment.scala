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

import scala.collection.mutable.Map

import tbd.{Adjustable, Mutator}
import tbd.master.Main
import tbd.mod.Mod

class Experiment(options: Map[Symbol, Any]) {
  val counts = options('counts).asInstanceOf[Array[Int]]
  val descriptions = options('descriptions).asInstanceOf[Array[String]]
  val percents = options('percents).asInstanceOf[Array[Double]]
  val repeat = options('repeat).asInstanceOf[Int]

  val runtime = Runtime.getRuntime()
  val rand = new scala.util.Random()

  print("desc\tpages\tinitial")
  for (percent <- percents) {
    print("\t" + (percent * 100) + "%")
  }
  print("\n")

  def warmUp() {
    val main = new Main()
    loadXML()
    addInput(0, 200, main)
    val results = Map[String, Double]()
    once(new WCAdjust(8), 200, results, main)
    main.shutdown()
  }

  def round(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  val pages = scala.collection.mutable.Map[String, String]()
  def loadXML(prefix: String = "") {
    val xml = scala.xml.XML.loadFile("wiki.xml")

    (xml \ "elem").map(elem => {
      (elem \ "key").map(key => {
	(elem \ "value").map(value => {
	  pages += (prefix + key.text -> value.text)
        })
      })
    })
  }

  def addInput(start: Int, stop: Int, main: Main) {
    val mutator = new Mutator(main)

    var i = start
    while (i < stop) {
      assert(pages.head._2 != null)
      mutator.put(i, pages.head._2)
      pages -= pages.head._1
      if (pages.size == 0) {
        loadXML()
      }
      i += 1
    }

    mutator.shutdown()
  }

  def runControl(description: String, control: (Int, Int) => Long) {
    allResults(description) = Map[Int, Map[String, Double]]()
    for (count <- options('counts).asInstanceOf[Array[Int]]) {
      allResults(description)(count) = Map[String, Double]()
      val time = control(count, repeat)
      for (percent <- "initial" +: percents) {
        allResults(description)(count)(percent+"") = time
      }
    }
  }

  def once(
      adjust: Adjustable,
      count: Int,
      results: Map[String, Double],
      main: Main) {
    val mutator = new Mutator(main)

    val before = System.currentTimeMillis()
    //val memBefore = (runtime.totalMemory - runtime.freeMemory) / (1024 * 1024)
    mutator.run(adjust)
    val initialElapsed = System.currentTimeMillis() - before

    if (results.contains("initial")) {
      results("initial") += initialElapsed
    } else {
      results("initial") = initialElapsed
    }

    //results("initialMem") = math.max((runtime.totalMemory - runtime.freeMemory) / (1024 * 1024) - memBefore,
    //                                 results("initialMem"))

    //while (true)
    for (percent <- percents) {
      var i =  0
      while (i < percent * count) {
        mutator.update(rand.nextInt(count), pages.head._2)
        pages -= pages.head._1
        if (pages.size == 0) {
          loadXML()
        }

        i += 1
      }
      val before2 = System.currentTimeMillis()
      mutator.propagate()

      if (results.contains(percent + "")) {
        results(percent + "") += System.currentTimeMillis() - before2
      } else {
        results(percent + "") = System.currentTimeMillis() - before2
      }

      //val memUsed = (runtime.totalMemory - runtime.freeMemory) / (1024 * 1024) - memBefore
      //results(percent + "Mem") = math.max(memUsed, results(percent + "Mem"))
    }

    mutator.shutdown()
  }

  val allResults = Map[String, Map[Int, Map[String, Double]]]()
  def run(adjust: Adjustable, description: String) {
    val main = new Main()
    loadXML()

    // We load the data for the first experiment when we do the warmup run.
    var lastCount = 0

    val resultsByCount = Map[Int, Map[String, Double]]()
    for (count <- counts) {
      addInput(lastCount, count, main)

      var run = 0
      val results = Map[String, Double]()
      while (run < repeat) {
        once(adjust, count, results, main)
        run += 1
      }

      print(description + "\t" + count + "\t")
      print(round(results("initial") / repeat))
      //print("\t" + round(results("initialMem")))

      for (percent <- percents) {
        print("\t" + round(results(percent + "") / repeat))
        //print("\t" + round(results(percent + "Mem")))
      }
      print("\n")

      resultsByCount(count) = results
    }

    allResults(description) = resultsByCount
    main.shutdown()
  }

  def printFormattedResults() {
    for (percent <- "initial" +: percents) {
      print(percent + "\t")
      for (description <- descriptions) {
        print(description + "\t")
      }
      print("\n")

      for (count <- counts) {
        print(count + "\t")

        for (description <- descriptions) {
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
  """

  def main(args: Array[String]) {
    def nextOption(map : Map[Symbol, Any], list: List[String]): Map[Symbol, Any] = {
      list match {
        case Nil => map
        case "--repeat" :: value :: tail =>
          nextOption(map ++ Map('repeat -> value.toInt), tail)
        case "--counts" :: value :: tail =>
          nextOption(map ++ Map('counts -> value.split(",").map(_.toInt)), tail)
        case "--descriptions" :: value :: tail =>
          nextOption(map ++ Map('descriptions -> value.split(",")), tail)
        case "--percents" :: value :: tail =>
          nextOption(map ++ Map('percents -> value.split(",").map(_.toDouble)), tail)
        case "--partitions" :: value :: tail =>
          nextOption(map ++ Map('partitions ->  value.toInt), tail)
        case "--algorithm" :: value :: tail =>
          nextOption(map ++ Map('algorithm -> value), tail)
        case option :: tail => println("Unknown option " + option + "\n" + usage)
                               exit(1)
      }
    }
    val options = nextOption(Map('repeat -> 3,
                                 'counts -> Array(200, 400, 600),
                                 'percents -> Array(.01, .05, .1),
                                 'partitions -> 10,
                                 'descriptions -> Array("smap", "seq-map", "par-map"),
                                 'algorithm -> "map"),
                             args.toList)
    val partitions = options('partitions).asInstanceOf[Int]
    val descriptions = options('descriptions).asInstanceOf[Array[String]]

    val experiment = new Experiment(options)
    experiment.warmUp()
    for (description <- descriptions) {
      if (options('algorithm) == "map") {
        if (description == "smap") {
          experiment.runControl("smap", SimpleMap.run)
        } else if (description == "seq-map") {
          experiment.run(new MapAdjust(partitions), "seq-map")
        } else if (description == "par-map") {
          experiment.run(new MapParAdjust(partitions), "par-map")
        }
      } else {
        experiment.run(new WCAdjust(partitions), "seq")
        experiment.run(new WCParAdjust(partitions), "par")
      }
    }

    experiment.printFormattedResults()
  }
}
