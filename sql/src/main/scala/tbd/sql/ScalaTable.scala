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
package tbd.sql

import scala.reflect.runtime.{ universe => ru}
import tbd.{Adjustable, Context, Mod, Mutator}
import tbd.datastore.IntData
import tbd.list.{ListConf, ListInput}
import tbd.TBD._

case class ScalaColumn (val columnName: String, val index: Int,
  val colType: ru.Type)

class ScalaTable (
  val rows: Iterator[Seq[Datum]],
  val colnameMap: Map[String, ScalaColumn],
  val table: SerTable,
  val mutator: Mutator,
  val listConf: ListConf) {

  val input = mutator.createList[Int, Seq[Datum]](listConf)

  rows.zipWithIndex.foreach( row => {
    input.put(row._2, row._1)
  })

  def printTable = {
    print(input.getAdjustableList.toBuffer(mutator))
  }
}