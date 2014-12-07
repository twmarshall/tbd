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
import tbd.{Adjustable, Context, Mod, Mutator}
import tbd.list.ListConf
import tbd.master.{Master, MasterConnector}

case class Rec (val manipulate: String, val pairkey: Int, val pairvalue: Double)

object SQLTest {
  def main (args: Array[String]) {
    val connector = MasterConnector("akka.tcp://masterSystem0@192.168.1.107:2552/user/master")
    val listConf =  new ListConf(partitions = 2, chunkSize = 1)
    val mutator = new Mutator(connector)
    val sqlContext = new TBDSqlContext(mutator)
    var f = (row: Array[String])  => Rec(row(0), row(1).trim.toInt, row(2).trim.toDouble)
    val tableName1 = "records1"
    val tableName2 = "records2"
    val path = "sql/data.csv"

    sqlContext.csvFile[Rec](tableName1, path, f, listConf)
    sqlContext.csvFile[Rec](tableName2, path, f)
    val statement = "select  * from records1 "
    val oper = sqlContext.sql(statement)
    println(oper.toBuffer)
    mutator.shutdown()
  }
}