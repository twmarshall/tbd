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
package tdb.scripts

import java.io._
import org.rogach.scallop._
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source

import tdb.Constants._
import tdb.datastore.berkeleydb.BerkeleyDatabase
import tdb.util._

object NaiveBerkeleyMap {
  def mapper(key: String, value: String): (String, Int) = {
    var count = 0
    for (word <- value.split("\\W+")) {
      count += 1
    }
    (key, count)
  }

  def writeOutput(fileName: String, output: Map[String, Int]) {
    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(fileName + ".txt"), "utf-8"))
    for ((key, value) <- output.toBuffer.sortWith(_._1 < _._1)) {
      writer.write(key + " -> " + value + "\n")
    }
    writer.close()
  }

  def main(args: Array[String]) {

    object Conf extends ScallopConf(args) {
      version("TDB 0.1 (c) 2014 Carnegie Mellon University")
      val file = opt[String]("file", 'f', default = Some("enwiki.xml"),
        descr = "The file to read.")
      val output = opt[String]("output", 'o',
        default = Some("naive-map-output"),
        descr = "The file to write the output to, .txt will be appended.")
      val partitions = opt[Int]("partitions", 'p', default = Some(12),
        descr = "Number of partitions.")
       val updateFile = opt[String]("updateFile", default = Some("updates.xml"),
        descr = "The file to read updates from.")
      val updates = opt[List[Int]]("updates", 'u', default = Some(List()),
        descr = "The number of updates to perform for each round of change" +
        "propagation")
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    val database = new BerkeleyDatabase("/tmp/tdb_berkeleydb")
    val inputStore = {
      val hashRange = new HashRange(0, Conf.partitions(), Conf.partitions())
      database.createInputStore(Conf.file(), hashRange)
    }

    println("count = "  + inputStore.count())
    if (inputStore.count() == 0) {
      println("load")
      val file = new File(Conf.file())
      val fileSize = file.length()

      val process = (key: String, value: String) => {
        inputStore.put(key, value)
      }

      FileUtil.readKeyValueFile(
        Conf.file(), fileSize, 0, fileSize, process)
    }

    val outputs = Buffer[Map[String, Int]]()
    val futures = Buffer[Future[Any]]()

    val before = System.currentTimeMillis()
    inputStore.foreachPartition {
      case index =>
        val output = Map[String, Int]()
        outputs += output
        futures += Future {
          val it = index.entities().iterator
          while (it.hasNext) {
            val entity = it.next
            output += mapper(entity.key, entity.value)
          }
        }
    }
    Await.result(Future.sequence(futures), 10000.seconds)
    println("time = " + (System.currentTimeMillis() - before))

    val totalOutput = Map[String, Int]()
    for (output <- outputs) {
      totalOutput ++= output
    }
    writeOutput(Conf.output(), totalOutput)

    if (Conf.updates().size > 0) {
      val updateFile = Source.fromFile(Conf.updateFile())
      var lines = updateFile.getLines

      for (update <- Conf.updates()) {
        for (i <- 1 to update) {
          val split = lines.next.split(unitSeparator)
          totalOutput += mapper(split(0), split(1))
        }

        writeOutput(Conf.output() + update, totalOutput)
      }
    }
  }
}
