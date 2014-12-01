package tbd.sql

import net.sf.jsqlparser.expression._;
import net.sf.jsqlparser.expression.operators.arithmetic._;
import net.sf.jsqlparser.expression.operators.conditional._;
import net.sf.jsqlparser.expression.operators.relational._;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

object SELECTTYPE extends Enumeration {
  type SELECTTYPE = Value
  val ALLCOLUMNS, ALLTABLECOLUMNS, SELECTEXPRESSIONITEM = Value
}

class SelectItemParseVisitor extends SelectItemVisitor {
  import tbd.sql.SELECTTYPE._
  var selectType : SELECTTYPE = _
  
    override def visit(arg: AllColumns) {
    selectType = SELECTTYPE.ALLCOLUMNS
  }

  override def visit(arg: AllTableColumns) {
    selectType = SELECTTYPE.ALLTABLECOLUMNS
  }

  override def visit(arg: SelectExpressionItem) {
    selectType = SELECTTYPE.SELECTEXPRESSIONITEM
    
  }
    def getItemType: SELECTTYPE = selectType
  
}