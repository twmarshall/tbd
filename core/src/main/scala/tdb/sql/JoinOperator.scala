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

import net.sf.jsqlparser.expression._
import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map
import scala.collection.JavaConversions._

import tdb._
import tdb.list._


class JoinAdjust (
  leftList: AdjustableList[Int, Seq[Datum]],
  rightList: AdjustableList[Int, Seq[Datum]],
  val conditionExp: Expression,
  val childTupleTableMap: List[String])
  extends Adjustable[AdjustableList[Int, Seq[Datum]]] {

  def condition (pair1: (Int, Seq[Datum]), pair2: (Int, Seq[Datum])): Boolean = {
    if (conditionExp == null) return true
    val row = pair1._2 ++ pair2._2
      val eval = new Evaluator(row.toArray, childTupleTableMap)
      conditionExp.accept(eval)
      eval.getAccumulatorBoolean
  }

  def run (implicit c: Context) = {
    leftList.join(rightList, condition).map(pair => {
      (pair._1, pair._2._1 ++ pair._2._2)
    })
  }
}

class JoinOperator (
  val leftOper: Operator,
  val rightOper: Operator,
  val conditionExp: Expression)
  extends Operator {

  var childOperators = List[Operator](leftOper, rightOper)
  val leftTable = leftOper.getTable
  val rightTable = rightOper.getTable
  var leftAdjustable : AdjustableList[Int,Seq[tdb.sql.Datum]] = _
  var rightAdjustable : AdjustableList[Int,Seq[tdb.sql.Datum]] = _
  var outputAdjustable : AdjustableList[Int,Seq[tdb.sql.Datum]] = _
  var isTupleMapPresent = true

  var tupleTableMap = List[String]()
  override def getTupleTableMap = tupleTableMap

  override def processOp () {
    childOperators.foreach(child => child.processOp)
    val childTupleTableMap = rightOper.getTupleTableMap ++ leftOper.getTupleTableMap
    tupleTableMap = childTupleTableMap
    leftAdjustable = leftOper.getAdjustable
    rightAdjustable = rightOper.getAdjustable

    val adjustable = new JoinAdjust(leftAdjustable, rightAdjustable,
      conditionExp, childTupleTableMap)
    outputAdjustable = leftTable.mutator.run(adjustable)
  }

  override def getTable: ScalaTable = leftTable

  override def getAdjustable: tdb.list.AdjustableList[Int,Seq[tdb.sql.Datum]] =
    outputAdjustable

  override def toBuffer = outputAdjustable.toBuffer(leftOper.getTable.mutator).
    map(_._2.map(BufferUtils.getValue))
}
