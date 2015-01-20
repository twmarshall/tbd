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
package thomasdb.sql

import net.sf.jsqlparser.schema.Table;

class TableScanOperator (
  val tableDef: net.sf.jsqlparser.schema.Table,
  val tablesMap: Map[String, ScalaTable])
  extends Operator{
  val table = tablesMap.get(tableDef.getName()).get
  val inputAdjustable = table.input.getAdjustableList
  var outputAdjustable = inputAdjustable

  var childOperators = List[Operator]();

  var tupleTableMap = List[String]()

  /*
   * set the list of column names
   */
  def setTupleTableMap (hasJoinCondition: Boolean = false) = {
    val alias = table.table.getAlias
    val colIter = table.colnameMap.keysIterator
    while (colIter.hasNext) {
      val datumnColumn = colIter.next
      if (alias != null) {
        tupleTableMap = tupleTableMap :+ (alias.toLowerCase + "." + datumnColumn)
      } else if (hasJoinCondition) {
        tupleTableMap = tupleTableMap :+ (table.table.getName + "." + datumnColumn)
      } else {
        tupleTableMap = tupleTableMap :+ datumnColumn
      }
    }
  }

  override def getTupleTableMap = tupleTableMap

  override def processOp () = {}

  override def getTable: ScalaTable =  table

  override def getAdjustable: thomasdb.list.AdjustableList[Int,Seq[thomasdb.sql.Datum]] =
    outputAdjustable

  override def toBuffer = outputAdjustable.toBuffer(table.mutator).
    map(_._2.map(BufferUtils.getValue))
}
