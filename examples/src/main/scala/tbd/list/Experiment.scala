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

import tbd.{Adjustable, Mutator}
import tbd.master.Main
import tbd.mod.Mod

class Experiment(
    aAlgorithm: String,
    aChunkSize: Int,
    aCount: Int,
    aPartition: Int,
    aPercent: Double,
    warmup: Boolean) {
  val algorithm = aAlgorithm
  val chunkSize = aChunkSize
  val count = aCount
  val partition = aPartition
  val percent = aPercent

  val chunks = ArrayBuffer[String]()
  def loadFile() {
    val source = scala.io.Source.fromFile("input.txt")

    val bb = new Array[Byte](chunkSize)
    val bis = new BufferedInputStream(new FileInputStream(new File("input.txt")))
    var bytesRead = bis.read(bb, 0, chunkSize)

    while (bytesRead > 0) {
      chunks += new String(bb)
      bytesRead = bis.read(bb, 0, chunkSize)
    }

    source.close()
  }

  val rand = new scala.util.Random()
  val maxKey = count * 2
  def addValue(mutator: Mutator, table: Map[Int, String]) {
    if (chunks.size == 0) {
      loadFile()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
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
      mutator.remove(key)
      table -= key
    } else {
      addValue(mutator, table)
    }
  }

  def updateValue(mutator: Mutator, table: Map[Int, String]) {
    if (chunks.size == 0) {
      loadFile()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    mutator.update(key, chunks.head)
    table(key) = chunks.head
    chunks -= chunks.head
  }

  def update(mutator: Mutator, table: Map[Int, String]) {
    rand.nextInt(3) match {
      case 0 => addValue(mutator, table)
      case 1 => removeValue(mutator, table)
      case 2 => updateValue(mutator, table)
    }
  }

  def run() {
    val results = Map[String, Double]()

    if (algorithm == "smap") {
      val time = SimpleMap.run(chunkSize, count)
      results("initial") = time
      results("propagation") = time
    } else {
      val mutator = new Mutator()
      val table = Map[Int, String]()

      while (table.size < count) {
        addValue(mutator, table)
      }

      val alg = algorithm match {
        case "map" => new MapAdjust(partition, false, false)
        case "parmap" => new MapAdjust(partition, true, false)
        case "memomap" => new MapAdjust(partition, false, true)
        case "memoparmap" => new MapAdjust(partition, true, true)
        case "filter" => new FilterAdjust(partition, false, false)
        case "parfilter" => new FilterAdjust(partition, true, false)
        case "memofilter" => new FilterAdjust(partition, false, true)
        case "memoparfilter" => new FilterAdjust(partition, true, true)
      }

      val before = System.currentTimeMillis()
      alg.initialRun(mutator)
      val initialElapsed = System.currentTimeMillis() - before

      assert(alg.checkOutput(table))

      results("initial") = initialElapsed

      var i =  0
      while (i < percent * count) {
        i += 1
        update(mutator, table)
      }

      val before2 = System.currentTimeMillis()
      mutator.propagate()
      val elapsed = System.currentTimeMillis() - before2

      assert(alg.checkOutput(table))

      results("propagation") = elapsed

      mutator.shutdown()
    }

    println(algorithm + "\t" + count + "\t" + percent + "\t" + results)

    if (!warmup) {
      Experiment.allResults += (this -> results)
    }
  }

  override def toString: String =
    "Experiment: " + algorithm + " " + chunkSize + " " + count + " " +
      partition + " " + percent
}

object Experiment {
  val usage = """
    Usage: run.sh [--repeat num] [--counts int,int,...] [--percents float,float,...]
      [--chunkSize int] [--descriptions seq,par,memo,...] [--partitions int]
  """

  val conf = new ExperimentConf()

  val allResults = Map[Experiment, Map[String, Double]]()

  def round(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def printTimeByCountByAlgorithm() {
    for (percent <- "initial" +: conf.percents) {
      print(percent + "\t")
      for (algorithm <- conf.algorithms) {
        print(algorithm + "\t")
      }
      print("\n")

      for (count <- conf.counts) {
        print(count)

        for (algorithm <- conf.algorithms) {
          var total = 0.0
          var repeat = 0

          for ((experiment, results) <- allResults) {
            if (experiment.algorithm == algorithm && experiment.count == count) {
              if (percent == "initial") {
                total += results("initial")
                repeat += 1
              } else if (experiment.percent == percent) {
                total += results("propagation")
                repeat += 1
              }
            }
          }

          print("\t" + round(total / repeat))
        }
        print("\n")
      }
    }
  }

  def main(args: Array[String]) {
    def parse(list: List[String]) {
      list match {
        case Nil =>
        case "--algorithms" :: value :: tail =>
          conf.algorithms = value.split(",")
          parse(tail)
        case "--chunkSizes" :: value :: tail =>
          conf.chunkSizes = value.split(",").map(_.toInt)
          parse(tail)
        case "--counts" :: value :: tail =>
          conf.counts = value.split(",").map(_.toInt)
          parse(tail)
        case "--partitions" :: value :: tail =>
          conf.partitions = value.split(",").map(_.toInt)
          parse(tail)
        case "--percents" :: value :: tail =>
          conf.percents = value.split(",").map(_.toDouble)
          parse(tail)
        case "--repeat" :: value :: tail =>
          conf.repeat = value.toInt
          parse(tail)
        case option :: tail => println("Unknown option " + option + "\n" + usage)
      }
    }
    parse(args.toList)

    for (i <- 0 to conf.repeat) {
      if (i == 0) {
        println("warmup")
      } else if (i == 1) {
        println("done warming up")
      }

      for (algorithm <- conf.algorithms) {
        for (chunkSize <- conf.chunkSizes) {
          for (count <- conf.counts) {
            for (partition <- conf.partitions) {
              for (percent <- conf.percents) {
                val experiment = new Experiment(algorithm, chunkSize, count,
                                                partition, percent, i == 0)

                experiment.run()
              }
            }
          }
        }
      }
    }

    printTimeByCountByAlgorithm()
  }
}
