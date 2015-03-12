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

import org.rogach.scallop._

import tdb.Mutator
import tdb.util.GraphData

object GraphGenerator {
  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("TDB 0.1 (c) 2014 Carnegie Mellon University")
      val count = opt[Int]("count", 'c', default = Some(1000))
      val file = opt[String]("file", 'f', default = Some("data.txt"),
        descr = "The file to write to.")
      val updates = opt[List[String]]("updates", 'u', default = Some(List()),
        descr = "The number of updates to perform for each round of change" +
        "propagation")
    }

    val mutator = new Mutator()
    val input = mutator.createList[Int, Array[Int]]()

    val data = new GraphData(
      input, Conf.count(), List(""), Conf.updates(), Conf.file())
    data.generate()

    while (data.hasUpdates()) {
      data.update()
    }

    mutator.shutdown()
  }
}
