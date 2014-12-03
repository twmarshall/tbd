package tbd.sql

import scala.collection.JavaConversions._
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

class PhysicalPlan (
    val tablesMap: Map[String, ScalaTable], 
    val selectBody: SelectBody,
    val projectStmts: List[_],
    val fromItems: FromItem,
    val whereClause: Expression = null) {
  
  
  def execute(): Operator = {
    val plainSelect = selectBody.asInstanceOf[PlainSelect]
    val fromItemVisitor = new FromItemParseVisitor(tablesMap)
    fromItems.accept(fromItemVisitor)
    var oper = fromItemVisitor.getOperator
    
    val tableJoins = plainSelect.getJoins()
    if (tableJoins != null) {
      TupleStruct.setJoinCondition(true)
      val joinIter = tableJoins.iterator()
      while (joinIter.hasNext()){
        val joinTable = joinIter.next().asInstanceOf[Join]
        val fromItemVisitor = new FromItemParseVisitor(tablesMap)
        joinTable.getRightItem().accept(fromItemVisitor);
    
        var rightOper = fromItemVisitor.getOperator
        oper = new JoinOperator(oper, rightOper,
            joinTable.getOnExpression)
      }
    } else {
      TupleStruct.setJoinCondition(false)
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
    
    if (hasFunction || plainSelect.getGroupByColumnReferences() != null) {
      val groupByColumns = plainSelect.getGroupByColumnReferences()
      val groupByList = if (groupByColumns != null)
                            groupByColumns.toList
                        else null
      oper = new GroupByOperator(oper, groupByList, projectStmts)
    } else {
      oper = new ProjectionOperator(oper, projectStmts)  
    }
    
    
    
    if (plainSelect.getOrderByElements() != null) {
      val orderBy = plainSelect.getOrderByElements().toList
      oper = new OrderByOperator(oper, orderBy)
    }
      
    
    oper.processOp

    oper
  }
  
}