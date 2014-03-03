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

import tbd.{Adjustable, Mutator}
import tbd.mod.Mod

object Experiment {
  type Options = Map[Symbol, Any]
  val runtime = Runtime.getRuntime()

  def round(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def run(adjust: Adjustable, options: Options, description: String) {
    val counts = options('counts).asInstanceOf[Array[Int]]
    val percents = options('percents).asInstanceOf[Array[Double]]
    val runs = options('repeat).asInstanceOf[Int]

    val r = new scala.util.Random()

    for (count <- counts) {
      var run = 0
      val results = scala.collection.mutable.Map[String, Double]()
      results("initial") = 0
      results("initialMem") = 0
      for (percent <- percents) {
	      results(percent + "") = 0
        results(percent + "Mem") = 0
      }

      while (run < runs) {
	      val mutator = new Mutator()

	      val xml = scala.xml.XML.loadFile("wiki.xml")
	      var i = 0

	      val pages = scala.collection.mutable.Map[String, String]()
	      (xml \ "elem").map(elem => {
	        (elem \ "key").map(key => {
	          (elem \ "value").map(value => {
              if (i < count) {
		            mutator.put(key.text, value.text)
		            i += 1
              } else {
		            pages += (key.text -> value.text)
              }
            })
          })
	      })

	      val before = System.currentTimeMillis()
        val memBefore = (runtime.totalMemory - runtime.freeMemory) / (1024 * 1024)
        //println(memBefore)
	      val output = mutator.run[Mod[Map[String, Int]]](adjust)
	      results("initial") += System.currentTimeMillis() - before
        results("initialMem") = math.max((runtime.totalMemory - runtime.freeMemory) / (1024 * 1024) - memBefore,
                                         results("initialMem"))

	      for (percent <- percents) {
          var i =  0
          while (i < percent * count) {
            mutator.update(r.nextInt(count).toString, pages.head._2)
            pages -= pages.head._1
            i += 1
          }
          val before2 = System.currentTimeMillis()
          mutator.propagate()
	        results(percent + "") += System.currentTimeMillis() - before2
          val memUsed = (runtime.totalMemory - runtime.freeMemory) / (1024 * 1024) - memBefore
          results(percent + "Mem") = math.max(memUsed, results(percent + "Mem"))
          //println(memUsed)
	      }

	      run += 1
	      mutator.shutdown()
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
        case option :: tail => println("Unknown option " + option + "\n" + usage)
                               exit(1)
      }
    }
    val options = nextOption(Map('repeat -> 3,
				                         'counts -> Array(100, 200, 300, 400),
				                         'percents -> Array(.01, .05, .1),
                                 'partitions -> 8),
                             args.toList)

    print("desc\tpages\tinitial\tmem")
    for (percent <- options('percents).asInstanceOf[Array[Double]]) {
      print("\t" + (percent * 100) + "%\tmem")
    }
    print("\n")

    run(new WCAdjust(options('partitions).asInstanceOf[Int]), options, "non")

    run(new WCParAdjust(options('partitions).asInstanceOf[Int]), options, "par")


    println("New session, total memory = %s, max memory = %s, free memory = %s".format(
      runtime.totalMemory / (1024 * 1024), runtime.maxMemory / (1024 * 1024), runtime.freeMemory / (1024 * 1024)))
  }
}
