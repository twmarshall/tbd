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
package thomasdb.sql.test

import scala.io.Source
import scala.collection.mutable.{ ArrayBuffer, Buffer, Map }
import scala.util.control.Breaks._
import org.scalatest._

import thomasdb.{ Adjustable, Context, Mod, Mutator }
import thomasdb.list._
import thomasdb.ThomasDB._
import thomasdb.sql._

class JoinTests extends FlatSpec with Matchers {

  "Join Test" should "check the Join operator" in {
    val mutator = new Mutator()
    val sqlContext = new ThomasDBSqlContext(mutator)
    val f = (row: Array[String]) =>
      Rec(row(0), row(1).trim.toLong, row(2).trim.toDouble)
    val tableName1 = "records1"
    val tableName2 = "records2"
    val path = "data/data.csv"

    sqlContext.csvFile[Rec](tableName1, path, f)
    sqlContext.csvFile[Rec](tableName2, path, f)

    var statement = "select  r1.manipulate, r1.pairkey, r1.pairvalue " +
      "from records1 as r1 inner join records2 as r2 " +
      "on r1.manipulate=r2.manipulate where r1.pairkey > r2.pairkey and r1.pairkey < 50 "
    var oper = sqlContext.sql(statement)

    val inputs1 = Source.fromFile(path).getLines.map(_.split(",")).
      map(f).toList
    val inputs2 = Source.fromFile(path).getLines.map(_.split(",")).
      map(f).toList

    var inputs = List[Rec]()

    for (input2 <- inputs2)
      for (input1 <- inputs1)
          if (input1.manipulate == input2.manipulate &&
           input1.pairkey > input2.pairkey && input1.pairkey < 50 ){
            inputs = inputs :+ input1
    }

    oper.toBuffer should be(inputs.map(r =>
      Seq(r.manipulate, r.pairkey, r.pairvalue)).toBuffer)

    statement = "select  r1.manipulate, r1.pairkey, r1.pairvalue " +
      "from records1 as r1 inner join records2 as r2 " +
      "where r1.manipulate=r2.manipulate and r1.pairkey > r2.pairkey and r1.pairkey < 50"
    oper = sqlContext.sql(statement)

    oper.toBuffer should be(inputs.map(r =>
      Seq(r.manipulate, r.pairkey, r.pairvalue)).toBuffer)

    mutator.shutdown()
  }

  "Join Integrate Test" should "check the integration of Join operator " +
    " with other operators" in {
    val mutator = new Mutator()
    val sqlContext = new ThomasDBSqlContext(mutator)
    val f = (row: Array[String]) =>
      Rec(row(0), row(1).trim.toLong, row(2).trim.toDouble)
    val tableName1 = "records1"
    val tableName2 = "records2"
    val path = "data/data.csv"

    sqlContext.csvFile[Rec](tableName1, path, f)
    sqlContext.csvFile[Rec](tableName2, path, f)

    var statement =
      "select  r1.manipulate, count(r1.pairkey), max(r1.pairvalue) " +
      "from records1 as r1 inner join records2 as r2 " +
      "on r1.manipulate=r2.manipulate where r1.pairkey > r2.pairkey " +
      "group by r1.manipulate"
    var oper = sqlContext.sql(statement)

    val inputs1 = Source.fromFile(path).getLines.map(_.split(",")).
      map(f).toList
    val inputs2 = Source.fromFile(path).getLines.map(_.split(",")).
      map(f).toList

    var inputs = List[Rec]()

    for (input1 <- inputs1;
      input2 <- inputs2;
      if input1.manipulate == input2.manipulate;
      if input1.pairkey > input2.pairkey ){
        inputs = inputs :+ input1
    }
    val outputs =  inputs.groupBy(r => r.manipulate).map(pair => {
      Seq(pair._1, pair._2.length, pair._2.foldLeft(0.0)(_ max _.pairvalue))
    })
    oper.toBuffer should be(outputs.toBuffer.sortWith((r1, r2) =>
      r1(0).asInstanceOf[String].compareTo(r2(0).asInstanceOf[String]) < 0))

    mutator.shutdown()
  }

}
