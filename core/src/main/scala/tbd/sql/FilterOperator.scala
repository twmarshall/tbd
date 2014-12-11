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
import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map
import scala.collection.JavaConversions._

import tbd._
import tbd.datastore.IntData
import tbd.list._


class FilterAdjust (
  list: AdjustableList[Int, Seq[Datum]],
  val condition: Expression,
  val tupleTableMap: List[String])
  extends Adjustable[AdjustableList[Int, Seq[Datum]]] {

  def run (implicit c: Context) = {
    list.filter(pair => {
      val eval = new Evaluator(pair._2.toArray,  tupleTableMap)
      condition.accept(eval)
      eval.getAccumulatorBoolean
    })
  }
}

class FilterOperator (val inputOper: Operator, val condition: Expression)
  extends Operator{
  var childOperators = List[Operator]():+ inputOper;
  val table = inputOper.getTable
  var inputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  var outputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  var isTupleMapPresent = true
  var tupleTableMap = List[String]()
  override def getTupleTableMap = tupleTableMap

  override def processOp () {
    
    childOperators.foreach(child => child.processOp)

    inputAdjustable = inputOper.getAdjustable
    val childOperator = childOperators(0)
    val childTupleTableMap = childOperator.getTupleTableMap
    tupleTableMap = childTupleTableMap

    val adjustable = new FilterAdjust(inputAdjustable,
      condition, childTupleTableMap)
    outputAdjustable = table.mutator.run(adjustable)
  }

  override def getTable: ScalaTable = table

  override def getAdjustable: 
    tbd.list.AdjustableList[Int,Seq[tbd.sql.Datum]] = outputAdjustable

  override def toBuffer = outputAdjustable.toBuffer(table.mutator).
    map(_._2.map(BufferUtils.getValue))
}