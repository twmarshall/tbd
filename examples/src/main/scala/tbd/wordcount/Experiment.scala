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

object Experiment {
  type Options = Map[Symbol, Any]
  val runtime = Runtime.getRuntime()
  val rand = new scala.util.Random()

  def round(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  val pages = scala.collection.mutable.Map[String, String]()
  def loadXML() {
    pages.clear()
    val xml = scala.xml.XML.loadFile("wiki.xml")

    (xml \ "elem").map(elem => {
      (elem \ "key").map(key => {
	(elem \ "value").map(value => {
	  pages += (key.text -> value.text)
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
      i += 1
    }

    mutator.shutdown()
  }

  def once(
      adjust: Adjustable,
      count: Int,
      percents: Array[Double],
      results: Map[String, Double],
      main: Main) {
    val mutator = new Mutator(main)

    val before = System.currentTimeMillis()
    val memBefore = (runtime.totalMemory - runtime.freeMemory) / (1024 * 1024)
    mutator.run(adjust)
    val initialElapsed = System.currentTimeMillis() - before

    results("initial") += initialElapsed
    results("initialMem") = math.max((runtime.totalMemory - runtime.freeMemory) / (1024 * 1024) - memBefore,
                                     results("initialMem"))

    //while (true)
    for (percent <- percents) {
      var i =  0
      while (i < percent * count) {
        mutator.update(rand.nextInt(count), pages.head._2)
        pages -= pages.head._1
        i += 1
      }
      val before2 = System.currentTimeMillis()
      mutator.propagate()
      results(percent + "") += System.currentTimeMillis() - before2
      val memUsed = (runtime.totalMemory - runtime.freeMemory) / (1024 * 1024) - memBefore
      results(percent + "Mem") = math.max(memUsed, results(percent + "Mem"))
    }

    mutator.shutdown()
  }

  def run(adjust: Adjustable, options: Options, description: String) {
    val counts = options('counts).asInstanceOf[Array[Int]]
    val percents = options('percents).asInstanceOf[Array[Double]]
    val runs = options('repeat).asInstanceOf[Int]

    val main = new Main()
    loadXML()

    // We load the data for the first experiment when we do the warmup run.
    var lastCount = 0

    for (count <- counts) {
      addInput(lastCount, count, main)

      var run = 0
      val results = scala.collection.mutable.Map[String, Double]()
      results("initial") = 0
      results("initialMem") = 0
      for (percent <- percents) {
        results(percent + "") = 0
        results(percent + "Mem") = 0
      }

      while (run < runs) {
        once(adjust, count, percents, results, main)
        run += 1
      }

      print(description + "\t" + count + "\t")
      print(round(results("initial") / runs))
      print("\t" + round(results("initialMem")))

      for (percent <- percents) {
        print("\t" + round(results(percent + "") / runs))
        print("\t" + round(results(percent + "Mem")))
      }
      print("\n")
    }

    main.shutdown()
  }

  val usage = """
    Usage: run.sh [--repeat num] [--counts int,int,...] [--percents float,float,...]
  """

  def main(args: Array[String]) {
    def nextOption(map : Options, list: List[String]): Options = {
      list match {
        case Nil => map
        case "--repeat" :: value :: tail =>
          nextOption(map ++ Map('repeat -> value.toInt), tail)
        case "--counts" :: value :: tail =>
          nextOption(map ++ Map('counts -> value.split(",").map(_.toInt)), tail)
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
                                 'algorithm -> "map"),
                             args.toList)

    print("desc\tpages\tinitial\tmem")
    for (percent <- options('percents).asInstanceOf[Array[Double]]) {
      print("\t" + (percent * 100) + "%\tmem")
    }
    print("\n")

    val main = new Main()
    loadXML()
    // Warm up run.
    val results = scala.collection.mutable.Map[String, Double]()
    results("initial") = 0
    results("initialMem") = 0
    results("0.1") = 0
    results("0.1Mem") = 0
    addInput(0, 200, main)
    once(new WCAdjust(8), 200, Array(.1), results, main)
    main.shutdown()

    if (options('algorithm) == "map") {
      for (count <- options('counts).asInstanceOf[Array[Int]]) {
        SimpleMap.run(count, options('repeat).asInstanceOf[Int])
      }

      run(new MapAdjust(options('partitions).asInstanceOf[Int]), options, "non-map")
      run(new MapParAdjust(options('partitions).asInstanceOf[Int]), options, "par-map")
    } else {
      run(new WCAdjust(options('partitions).asInstanceOf[Int]), options, "non")
      run(new WCParAdjust(options('partitions).asInstanceOf[Int]), options, "par")
    }

  }
}
