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
package tdb.sql

import com.typesafe.config.ConfigFactory
import org.rogach.scallop._
import scala.util.control.Breaks._
import tdb.{Adjustable, Context, Mod, Mutator}
import tdb.list.ListConf
import tdb.master.{Master, MasterConnector}

case class Rec (val manipulate: String, val pairkey: Int, val pairvalue: Double)

object SQLTest {

  def main (args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("TDB 0.1 (c) 2014 Carnegie Mellon University")
      banner("Usage: sql.sh [options]")

      val master = opt[String]("master", 'm', default = Some(""),
        descr = "The master Ref")
      val partitionSize = opt[Int]("partitions", 's', default = Some(1),
        descr = "The partition size.")
      val path = opt[String]("path", 'p' , default = Some("core/data/data.csv"),
        descr = "The path to the csv file.")
    }

    val master = Conf.master.get.get
    val partitionSize = Conf.partitionSize.get.get
    val path = Conf.path.get.get

    val connector = if (master == "")
                      MasterConnector()
                    else MasterConnector(master)
    val listConf =  new ListConf(partitions = partitionSize, chunkSize = 1)
    val mutator = new Mutator(connector)
    val sqlContext = new TDBSqlContext(mutator)
    var f = (row: Array[String])  => Rec(row(0), row(1).trim.toInt, row(2).trim.toDouble)
    val tableName1 = "records"

    var statement = "select * from records"

    var before = System.currentTimeMillis()
    println("Loading data...")
    sqlContext.csvFile[Rec](tableName1, path, f, listConf)
    var elapsed = System.currentTimeMillis() - before
    println("Done loading data. Time elapsed:" + elapsed)

    println("Input a SQL statement:")
    for (statement <- scala.io.Source.stdin.getLines;
         if statement != null){
      before = System.currentTimeMillis()
      println("Running SQL...")
      val oper = sqlContext.sql(statement)
      elapsed = System.currentTimeMillis() - before

      BufferUtils.printOperator(oper)
      println("Done running. Time elapsed:" + elapsed)
      println("Input a SQL statement:")
    }
    mutator.shutdown()
    System.exit(0)
  }
}
