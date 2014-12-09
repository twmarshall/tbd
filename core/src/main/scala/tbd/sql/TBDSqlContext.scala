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

import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.runtime.{ universe => ru}
import scala.reflect.runtime.universe._
import scala.collection.JavaConversions._
import java.io.StringReader;
import java.util.Date;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.schema._;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import tbd.list.{ListInput, ListConf}
import tbd.{Adjustable, Context, Mod, Mutator}

class TableNameTakenException(
  tableName: String,
  nestedException: Throwable)
  extends Exception {
  def this() = this("", null)

  def this(tableName: String) =
    this(s"Table name $tableName is already taken", null)

  def this(nestedException : Throwable) = this("", nestedException)
}

class TBDSqlContext(val mutator: Mutator)  {

  var tablesMap: Map[String, ScalaTable] = Map()

  def T2Datum[T: ClassTag: TypeTag]
      (table: net.sf.jsqlparser.schema.Table,
       t: T, members: Iterable[ru.Symbol]) : Seq[Datum] = {
    val m = ru.runtimeMirror(t.getClass.getClassLoader)
    val im = m.reflect(t)
    members.map( mem => {
      val termSymb = ru.typeOf[T].decl(ru.TermName(mem.name.toString)).asTerm
      val tFieldMirror = im.reflectField(termSymb)

      val colName = mem.name.toString().trim()
      tFieldMirror.get match {
        case l: Long =>
          new Datum.dLong(tFieldMirror.get.toString,
            new Column(table, colName))
        case i: Int =>
          new Datum.dLong(tFieldMirror.get.toString,
            new Column(table,colName))
        case d: Double =>
          new Datum.dDecimal(tFieldMirror.get.toString,
            new Column(table, colName))
        case f: Float =>
          new Datum.dDecimal(tFieldMirror.get.toString,
            new Column(table, colName))
        case d: java.util.Date =>
          new Datum.dDate(tFieldMirror.get.asInstanceOf[Date],
            new Column(table, colName))
        case _ =>
          new Datum.dString(tFieldMirror.get.toString,
            new Column(table,colName))
      }
    }).toSeq

  }

  def csvFile[T: ClassTag: TypeTag]
      (tableName: String,
       path: String,
       f: Array[String] => T,
       listConf: ListConf  = new ListConf(partitions = 1, chunkSize = 1)) = {
    if (tablesMap.contains(tableName)) throw 
      new TableNameTakenException(tableName)
    val fileContents = Source.fromFile(path).getLines.map(_.split(",")).map(f)
    val members = typeOf[T].members.filter(!_.isMethod)
    val colnameMap: Map[String, ScalaColumn] = 
      members.zipWithIndex.map( x => (
        x._1.name.toString(),
        ScalaColumn(x._1.name.toString(), x._2, x._1.typeSignature))
      ).toMap

    val table = new Table(tableName, tableName)
    val listContents = fileContents.map(x => T2Datum[T](table, x, members))

    tablesMap += (tableName -> new  ScalaTable(listContents,
      colnameMap, table, mutator, listConf))

  }

  def sql (query: String): Operator = {
    val parserManager = new CCJSqlParserManager();
    val statement = parserManager.parse(
      new StringReader(query)).asInstanceOf[Select];

    val oper = if (statement.isInstanceOf[Select]) {
      val selectStmt = statement.asInstanceOf[Select]
              .getSelectBody();
      if (selectStmt.isInstanceOf[PlainSelect]) {
        val plainSelect = selectStmt.asInstanceOf[PlainSelect]

        val projectStmts = plainSelect.getSelectItems().toList

        val whereClause = plainSelect.getWhere()

        val physicalPlan = new PhysicalPlan(tablesMap,
          selectStmt, projectStmts, plainSelect.getFromItem(), whereClause)
        physicalPlan.execute()
      } else {
        None.asInstanceOf[Operator]
      }
    } else {
      None.asInstanceOf[Operator]
    }
    oper
  }
}