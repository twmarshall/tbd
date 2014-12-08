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
package tbd.sql.test

import scala.io.Source
import scala.collection.mutable.{ ArrayBuffer, Buffer, Map }
import org.scalatest._

import tbd.{ Adjustable, Context, Mod, Mutator }
import tbd.datastore.IntData
import tbd.list._
import tbd.TBD._
import tbd.sql._

class SelectFromWhereTests extends FlatSpec with Matchers {

  "Select From Where Test" should "implement  TableScan, " +
    "Filter and Projection operator" in {
    val mutator = new Mutator()
    val sqlContext = new TBDSqlContext(mutator)
    val f = (row: Array[String]) =>
        Rec(row(0), row(1).trim.toLong, row(2).trim.toDouble)
    val tableName = "records"
    val path = "data.csv"
    sqlContext.csvFile[Rec](tableName, path, f)

    var statement = "select   * from records"
    var oper = sqlContext.sql(statement)

    var inputs = Source.fromFile(path).getLines.map(_.split(",")).
        map(f).map(r => Seq(r.pairvalue, r.pairkey, r.manipulate))

    oper.toBuffer should be(inputs.toBuffer)

    statement = "select   2* pairvalue, pairkey from records"
    oper = sqlContext.sql(statement)

    inputs = Source.fromFile(path).getLines.map(_.split(",")).
        map(f).map(r => Seq(r.pairvalue * 2, r.pairkey))

    oper.toBuffer should be(inputs.toBuffer)

    statement = "select   * from records where pairvalue > 10"
    oper = sqlContext.sql(statement)

    inputs = Source.fromFile(path).getLines.map(_.split(",")).
        map(f).filter(r =>
            r.pairvalue > 10).map(r => Seq(r.pairvalue, r.pairkey, r.manipulate))

    oper.toBuffer should be(inputs.toBuffer)

    statement = "select   2* pairvalue, pairkey " +
        "from records where pairvalue > 10"
    oper = sqlContext.sql(statement)

    inputs = Source.fromFile(path).getLines.map(_.split(",")).
        map(f).filter(r =>
            r.pairvalue > 10).map(r => Seq(r.pairvalue * 2, r.pairkey))

    oper.toBuffer should be(inputs.toBuffer)
    mutator.shutdown()
  }

}
