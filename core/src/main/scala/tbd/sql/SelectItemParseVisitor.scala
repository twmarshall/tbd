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