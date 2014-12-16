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

class OrderByTests extends FlatSpec with Matchers {

  "Orderby Test" should "return the sorted rows" in {
    val mutator = new Mutator()
    val sqlContext = new TBDSqlContext(mutator)
    val f = (row: Array[String]) =>
      Rec(row(0), row(1).trim.toLong, row(2).trim.toDouble)
    val tableName = "records"
    val path = "data/data.csv"
    sqlContext.csvFile[Rec](tableName, path, f)

    var statement = "select   2* pairvalue, pairkey " +
      "from records where pairvalue > 10 order by pairkey"
    var oper = sqlContext.sql(statement)

    var inputs = Source.fromFile(path).getLines.map(_.split(",")).
      map(f).filter(r =>
      r.pairvalue > 10).map(r => Seq(r.pairvalue * 2, r.pairkey))

    oper.toBuffer should be(inputs.toBuffer.sortWith((r1, r2) =>
      r1(1).asInstanceOf[Long] < r2(1).asInstanceOf[Long]))

    statement = "select   2* pairvalue, pairkey " +
      "from records where pairvalue > 10 order by pairkey desc"
    oper = sqlContext.sql(statement)

    inputs = Source.fromFile(path).getLines.map(_.split(",")).
      map(f).filter(r =>
      r.pairvalue > 10).map(r => Seq(r.pairvalue * 2, r.pairkey))

    oper.toBuffer should be(inputs.toBuffer.sortWith((r1, r2) => 
      r1(1).asInstanceOf[Long] > r2(1).asInstanceOf[Long]))

    mutator.shutdown()
  }

}
