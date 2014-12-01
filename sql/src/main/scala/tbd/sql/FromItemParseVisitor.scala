package tbd.sql

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

class FromItemParseVisitor (val tablesMap: Map[String, ScalaTable])
  extends FromItemVisitor {
  
  var oper: Operator = _
  
  def visit(table: Table) {
    tablesMap.get(table.getName()).get.table.setAlias(table.getAlias)
    oper = new TableScanOperator(table, tablesMap)
  }
  
  def visit(subSelect: SubSelect) {
    
  }
  
  def visit(subJoin: SubJoin) {
    
  }
  
  def getOperator: Operator = oper
  
  
} 