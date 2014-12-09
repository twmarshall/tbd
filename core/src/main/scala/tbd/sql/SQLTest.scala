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
//case class Rec (val url: String, val rank: Int, val duration: Int)

object SQLTest {
    
  def main (args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("TBD 0.1 (c) 2014 Carnegie Mellon University")
      banner("Usage: sql.sh [options] sql")
      
      val master = opt[String]("master", 'm', default = Some(""),
        descr = "The master Ref")

      val partitionSize = opt[Int]("partitions", 's', default = Some(1),
        descr = "The partition size.")
      val path = opt[String]("path", 'p' , default = Some("sql/data.csv"),
        descr = "The path to the csv file.")
      //val sql = trailArg[String](required = true)
    }

      val master = Conf.master.get.get
      val partitionSize = Conf.partitionSize.get.get
      val path = Conf.path.get.get

    val connector = if (master == "")
                      MasterConnector()
                    else MasterConnector(master)
    val listConf =  new ListConf(partitions = partitionSize, chunkSize = 1)
    val mutator = new Mutator(connector)
    val sqlContext = new TBDSqlContext(mutator)
    var f = (row: Array[String])  => Rec(row(0), row(1).trim.toInt, row(2).trim.toDouble)
//    var f = (row: Array[String])  => Rec(row(0), row(1).trim.toInt, row(2).trim.toInt)
    val tableName1 = "records"
    //val path = "sql/data.csv"
   val statement = "select * from records" //"select pairkey, pairvalue*3.14- 2 from records "
//    	"where pairkey > 10 and pairvalue < 100 "

      var before = System.currentTimeMillis()
    println("Loading data...")
    sqlContext.csvFile[Rec](tableName1, path, f, listConf)
    var elapsed = System.currentTimeMillis() - before
    println("Done loading data. Time elapsed:" + elapsed)
//    sqlContext.csvFile[Rec](tableName2, path, f)


     before = System.currentTimeMillis()
    println("Running SQL...")
    val oper = sqlContext.sql(statement)
    elapsed = System.currentTimeMillis() - before
    println(oper.toBuffer)    
    println("Done running. Time elapsed:" + elapsed)
    mutator.shutdown()
  }
}