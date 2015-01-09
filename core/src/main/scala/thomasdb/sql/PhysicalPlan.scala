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

import scala.collection.JavaConversions._
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select._;

class PhysicalPlan (
    val tablesMap: Map[String, ScalaTable],
    val selectBody: SelectBody,
    val projectStmts: List[_],
    val fromItems: FromItem,
    val whereClause: Expression = null) {

  def execute(): Operator = {
    val plainSelect = selectBody.asInstanceOf[PlainSelect]
    val tableJoins = plainSelect.getJoins()
    val hasJoin =  (tableJoins != null)
    val fromItemVisitor = new FromItemParseVisitor(tablesMap)
    fromItems.accept(fromItemVisitor)
    var oper = fromItemVisitor.getOperator

    oper.asInstanceOf[TableScanOperator].setTupleTableMap(hasJoin)

    if (tableJoins != null) {
      val joinIter = tableJoins.iterator()
      while (joinIter.hasNext()){
        val joinTable = joinIter.next().asInstanceOf[Join]
        val fromItemVisitor = new FromItemParseVisitor(tablesMap)
        joinTable.getRightItem().accept(fromItemVisitor);

        var rightOper = fromItemVisitor.getOperator
        rightOper.asInstanceOf[TableScanOperator].setTupleTableMap(hasJoin)
        oper = new JoinOperator(oper, rightOper,
            joinTable.getOnExpression)
      }
    }
    if (whereClause != null) {
      oper = new FilterOperator(oper, whereClause)
    }

    var hasFunction = false;
    for(sei <- projectStmts) {
      if (sei.isInstanceOf[SelectExpressionItem]){
        val e = sei.asInstanceOf[SelectExpressionItem].getExpression();
        if(e.isInstanceOf[Function])
          hasFunction = true;
      }
    }

    if (plainSelect.getOrderByElements() != null) {
      val orderBy = plainSelect.getOrderByElements().toList
      oper = new OrderByOperator(oper, orderBy)
    }

    if (hasFunction || plainSelect.getGroupByColumnReferences() != null) {
      val groupByColumns = plainSelect.getGroupByColumnReferences()
      val groupByList = if (groupByColumns != null)
                            groupByColumns.toList
                        else null
      oper = new GroupByOperator(oper, groupByList, projectStmts)
    } else {
      oper = new ProjectionOperator(oper, projectStmts)
    }

    oper.processOp

    oper
  }
}
