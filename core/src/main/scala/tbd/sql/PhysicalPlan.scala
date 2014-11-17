package tbd.sql

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
    val whereClause: Expression = null,  
    val tableJoins: List[Any] = null,
    val orderByElements: List[Any] = null) {
  
  
  def execute() = {
    val fromItemVisitor = new FromItemParseVisitor(tablesMap)
    fromItems.accept(fromItemVisitor)
    var oper = fromItemVisitor.getOperator
    if (whereClause != null) {
      oper = new FilterOperator(oper, whereClause)
    }
    oper = new ProjectionOperator(oper, projectStmts)
    oper.processOp
    //val table = oper.getTable
    //table.printTable
    
  }
  
}