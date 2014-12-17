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

import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import scala.collection.{ GenIterable, GenMap, Seq }
import scala.collection.mutable.Map
import scala.collection.JavaConversions._

import tbd._
import tbd.datastore.IntData
import tbd.list._

/*
 * GroupBy operation execute in two steps:
 * 1. map out the rows by keys of groupby columns
 * 2. reduce the  rows by key, applying the aggregation functions
 */
class GroupByAdjust(
  list: AdjustableList[Int, Seq[Datum]],
  groupbyList: List[_],
  selectExpressionList: List[_],
  val tupleTableMap: List[String])
  extends Adjustable[AdjustableList[Int, Seq[Datum]]] {

  var aggreFuncTypes = Seq[String]()
  var hasAvg = false

  for (selectExpressionItem <- selectExpressionList) {
      val selectItem = selectExpressionItem.asInstanceOf[SelectExpressionItem]

      val e = selectItem.getExpression
      if (e.isInstanceOf[net.sf.jsqlparser.expression.Function]) {
        val funcName = e.asInstanceOf[net.sf.jsqlparser.expression.Function].
          getName()
        if (funcName.equalsIgnoreCase("avg")) {
          // AVG computes from the division of two aggregate functions:
          // SUM and COUNT
          aggreFuncTypes = aggreFuncTypes :+ "avgsum"
          aggreFuncTypes = aggreFuncTypes :+ "avgcount"
          hasAvg = true
        } else if (funcName.equalsIgnoreCase("count")) {
          aggreFuncTypes = aggreFuncTypes :+ "count"
        } else {
          aggreFuncTypes = aggreFuncTypes :+ funcName
        }
      } else {
        aggreFuncTypes = aggreFuncTypes :+ "col"

      }
  }

  /*
   * map out by groupby columns
   */
  def mapper(pair: (Int, Seq[Datum])): (Seq[Datum], Seq[Datum]) = {

    var row = pair._2
    var groupByDatumList = Seq[Datum]()
    var singleDatum: Datum = null.asInstanceOf[Datum]

    val datumColumnName = tupleTableMap
    var keyCols = Seq[Datum](new Datum.dLong(pair._1, null))
    var valCols = Seq[Datum]()
    if ( groupbyList != null) {
      for (groupByColumnName <- groupbyList) {
        val grpColName = groupByColumnName.asInstanceOf[Column].
          getWholeColumnName().toLowerCase
        if (datumColumnName.contains(grpColName)) {
          val index = datumColumnName.indexOf(grpColName)
          singleDatum = row(index)
          keyCols = keyCols :+ singleDatum
        }
      }
      } else {
        keyCols = keyCols :+ new Datum.dLong(0, null)
      }

    for (selectExpressionItem <- selectExpressionList) {
      val selectItem = selectExpressionItem.asInstanceOf[SelectExpressionItem]

      val e = selectItem.getExpression

      val eval = new Evaluator(pair._2.toArray, tupleTableMap)
      e.accept(eval);

      val newCol =
        if (selectItem.getAlias() != null) {
          new Column(null, selectItem.getAlias())
        } else if (eval.getColumn == null) {
          null.asInstanceOf[Column]
        } else{
          eval.getColumn()
        }
      val ob = eval.getResult()

      val datum =
        if (ob.isInstanceOf[Long])
          new Datum.dLong(ob.asInstanceOf[Long], newCol)
        else if (ob.isInstanceOf[Double])
          new Datum.dDecimal(ob.asInstanceOf[Double], newCol)
        else if (ob.isInstanceOf[java.util.Date])
          new Datum.dDate(ob.asInstanceOf[java.util.Date], newCol)
        else
          new Datum.dString(ob.toString, newCol)

      if (e.isInstanceOf[net.sf.jsqlparser.expression.Function]) {
        val funcName = e.asInstanceOf[net.sf.jsqlparser.expression.Function].
          getName()
        if (funcName.equalsIgnoreCase("avg")) {
          valCols = valCols :+ datum
          valCols = valCols :+ new Datum.dLong(1L, newCol)
        } else if (funcName.equalsIgnoreCase("count")) {
          valCols = valCols :+ new Datum.dLong(1L, newCol)
        } else {
          valCols = valCols :+ datum
        }
      } else {
        valCols = valCols :+ datum
      }
    } //end for selectItem

    (keyCols, valCols)
  }

  /*
   * compare to group by keys, in order to sort rows by the group by columns
   */
  def groupByComparator(
    pair1: (Seq[Datum], Seq[Datum]),
    pair2: (Seq[Datum], Seq[Datum])): Int = {

    var cmp = 0
    for (colIdx <- 1 until pair1._1.length) {

      cmp = pair1._1(colIdx).compareTo(pair2._1(colIdx))

      if (cmp != 0) return cmp
    }
    cmp
  }

  def reduceFunc(row1: Seq[Datum], row2: Seq[Datum]): Seq[Datum] = {

    var newDatumList = List[Datum]()
    for (i <- 0 until aggreFuncTypes.length) {
      val aggrFunc = aggreFuncTypes(i)
      val d = aggrFunc.toLowerCase() match {
        case "max" => if (row1(i).compareTo(row2(i)) > 0) row1(i) else (row2(i))
        case "min" => if (row1(i).compareTo(row2(i)) < 0) row1(i) else (row2(i))
        case "col" => row1(i)
        case _ => row1(i).sumDatum(row2(i))

      }
      newDatumList = newDatumList:+ d
    }
    newDatumList.toSeq
  }

  /*
   * Compute AVG from SUM and COUNT
   */
  def mapper2 (pair: (Seq[Datum], Seq[Datum])) : (Int, Seq[Datum]) = {

    val idx = pair._1(0).asInstanceOf[Datum.dLong].getValue().asInstanceOf[Int]
    if (! hasAvg) (idx, pair._2)
    else {
      var newDatumList = List[Datum]()
      for (i <- 0 until aggreFuncTypes.length) {
        if (aggreFuncTypes(i).equalsIgnoreCase("avgcount")) {

        } else if (! aggreFuncTypes(i).equalsIgnoreCase("avgsum")) {
          newDatumList = newDatumList :+ pair._2(i)
        } else {
          val sum = pair._2(i) match {
            case d: Datum.dLong =>
              pair._2(i).asInstanceOf[Datum.dLong].getValue()
            case d: Datum.dDecimal =>
              pair._2(i).asInstanceOf[Datum.dDecimal].getValue()
          }
          val count = pair._2(i+1).asInstanceOf[Datum.dLong].getValue()
          newDatumList = newDatumList :+
            new Datum.dDecimal(sum.asInstanceOf[Double]/count, pair._2(i).getColumn())
        }
      }
      (idx, newDatumList.toSeq)
    }
  }

  def run(implicit c: Context) = {
    list.map(mapper).reduceByKey(reduceFunc, groupByComparator).map(mapper2)
  }
}

/*
 * GroupByOperator implements the GROUP BY and AGGREGATION clauses
 */
class GroupByOperator(
  val inputOper: Operator,
  val groupbyList: List[_],
  val selectExpressionList: List[_])
  extends Operator {
  var childOperators = List[Operator]() :+ inputOper;
  val table = inputOper.getTable
  var inputAdjustable: AdjustableList[Int, Seq[tbd.sql.Datum]] = _
  var outputAdjustable: AdjustableList[Int, Seq[tbd.sql.Datum]] = _
  var isTupleMapPresent = true

  var tupleTableMap = List[String]()
  override def getTupleTableMap = tupleTableMap

  /*
   * set the list of column names
   */
  def setTupleTableMap (childTupleTableMap: List[String]) = {
    selectExpressionList.foreach(item => {
      val selectItem = item.asInstanceOf[SelectItem]
      if (selectItem.isInstanceOf[SelectExpressionItem]){
          val expItem = selectItem.asInstanceOf[SelectExpressionItem]
          var e = expItem.getExpression
          val newCol = if (e.isInstanceOf[Column])
                          e.asInstanceOf[Column].getWholeColumnName.toLowerCase
                       else expItem.getAlias()
          tupleTableMap = tupleTableMap :+ newCol
      }
    })
  }

  override def processOp() {
    childOperators.foreach(child => child.processOp)

    var inputAdjustable = inputOper.getAdjustable
    val childOperator = childOperators(0)
    val childTupleTableMap = childOperator.getTupleTableMap
    setTupleTableMap (childTupleTableMap)
    val adjustable = new GroupByAdjust(inputAdjustable,
      groupbyList, selectExpressionList, childTupleTableMap)
    outputAdjustable = table.mutator.run(adjustable)
  }

  override def getTable: ScalaTable = table

  override def getAdjustable:
    tbd.list.AdjustableList[Int, Seq[tbd.sql.Datum]] = outputAdjustable

  override def toBuffer = outputAdjustable.toBuffer(table.mutator).
    map(_._2.map(BufferUtils.getValue))
}
