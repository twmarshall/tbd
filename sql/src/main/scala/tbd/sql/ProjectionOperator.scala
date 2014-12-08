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
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem
import net.sf.jsqlparser.statement.select.SelectItem
import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map

import tbd._
import tbd.datastore.IntData
import tbd.list._


class ProjectionAdjust (
  list: AdjustableList[Int, Seq[Datum]],
  val projectStmt: List[_],
  var isTupleMapPresent: Boolean)
  extends Adjustable[AdjustableList[Int, Seq[Datum]]] {

  def run (implicit c: Context) = {

    list.map((pair: (Int, Seq[Datum])) => {

      val t = pair._2.toArray

      var newDatumList = List[Datum]()
      if (isTupleMapPresent) {
          TupleStruct.setTupleTableMap(pair._2.toArray)
          isTupleMapPresent = false;
      }
      projectStmt.foreach( item => {
        val selectItem = item.asInstanceOf[SelectItem]
        if (selectItem.isInstanceOf[SerAllColumns] ||
            selectItem.isInstanceOf[SerAllTableColumns]) {
          newDatumList = newDatumList ++ t.toList;
        }
        /*
        val sv = new SelectItemParseVisitor
        selectItem.accept(sv)

        
        sv.getItemType match {
          case SELECTTYPE.ALLCOLUMNS | SELECTTYPE.ALLTABLECOLUMNS =>
            newDatumList = newDatumList ++ t.toList;
          case SELECTTYPE.SELECTEXPRESSIONITEM => {
            */
            else {
            val expItem = selectItem.asInstanceOf[SerSelectExpressionItem]
            val e = expItem.getExpression()
            val eval = new Evaluator(t.toArray)
            e.accept(eval)
            val newCol = expItem.getAlias() match {
                case null => if (eval.getColumn != null) 
                                new SerColumn(eval.getColumn())
                             else null.asInstanceOf[SerColumn]
                case _ => new SerColumn(null, expItem.getAlias())
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

            newDatumList = newDatumList :+ datum
          //} //end case SELECTEXPRESSIONITEM
        } //end match sv.getItemType
        
      })
      (pair._1, newDatumList.toSeq)
    })
  }
}

class ProjectionOperator (val inputOper: Operator, val projectStmt: List[_])
    extends Operator{
  var childOperators = List[Operator]():+ inputOper;
  val table = inputOper.getTable
  var inputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  var outputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  val colnameMap = table.colnameMap
  var isTupleMapPresent = true

  override def processOp () {

    childOperators.foreach(child => child.processOp)

    inputAdjustable = inputOper.getAdjustable
    val adjustable = new ProjectionAdjust(inputAdjustable, projectStmt, isTupleMapPresent)  
    outputAdjustable = table.mutator.run[AdjustableList[Int,Seq[tbd.sql.Datum]]](adjustable)
  }

  override def getTable: ScalaTable = table

  override def getAdjustable: tbd.list.AdjustableList[Int,Seq[tbd.sql.Datum]] =
    outputAdjustable
  override def toBuffer = outputAdjustable.toBuffer(table.mutator).
    map(_._2.map(BufferUtils.getValue))
}