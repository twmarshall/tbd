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

import com.typesafe.config.ConfigFactory
import org.rogach.scallop._
import tbd.{Adjustable, Context, Mod, Mutator}
import tbd.list.ListConf
import tbd.master.{Master, MasterConnector}

case class Rec (val manipulate: String, val pairkey: Int, val pairvalue: Double)

object SQLTest {
    
  def main (args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("TBD 0.1 (c) 2014 Carnegie Mellon University")
      banner("Usage: sql.sh [options] sql")
      /*
      val sql = opt[String]("sql", 'q', default = Some("select pairkey from records"),
        descr = "The sql statement.")
*/
      val partitionSize = opt[Int]("partitions", 's', default = Some(1),
        descr = "The partition size.")
      val path = opt[String]("path", 'p' , default = Some("sql/data.csv"),
        descr = "The path to the csv file.")
      val master = trailArg[String](required = false)
    }

      val master = Conf.master.get.get
      val statement = "select * from records"
      val partitionSize = Conf.partitionSize.get.get
      val path = Conf.path.get.get

    val connector = MasterConnector()
    val listConf =  new ListConf(partitions = partitionSize, chunkSize = 1)
    val mutator = new Mutator(connector)
    val sqlContext = new TBDSqlContext(mutator)
    var f = (row: Array[String])  => Rec(row(0), row(1).trim.toInt, row(2).trim.toDouble)
    val tableName1 = "records"
    //val path = "sql/data.csv"

    var before = System.currentTimeMillis()
    sqlContext.csvFile[Rec](tableName1, path, f, listConf)
    var elapsed = System.currentTimeMillis() - before
    println("Done loading. Time elapsed: " + elapsed)
//    sqlContext.csvFile[Rec](tableName2, path, f)
//    val statement = "select  * from records1 "
    before = System.currentTimeMillis()
   // output = mutator.run[Output](adjust)

    val oper = sqlContext.sql(statement)
    elapsed = System.currentTimeMillis() - before
        println("Done running. Time elapsed: " + elapsed)
    //println(oper.toBuffer)
    mutator.shutdown()
  }
}