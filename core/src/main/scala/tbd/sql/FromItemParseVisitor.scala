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

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

/*
 * Entry to visiting the FROM items (table, sub select, etc)
 * retrieves the operator that scan the schema speficied by the
 * FROM clause
 */
class FromItemParseVisitor (val tablesMap: Map[String, ScalaTable])
  extends FromItemVisitor {

  var oper: Operator = _

  /*
   * Get the operator that scans the table,
   * set alias if any.
   */
  def visit(table: Table) {
    tablesMap.get(table.getName()).get.table.setAlias(table.getAlias)
    oper = new TableScanOperator(table, tablesMap)
  }

  def visit(subSelect: SubSelect): Unit = ???

  def visit(subJoin: SubJoin): Unit = ???

  def visit(valuesList: net.sf.jsqlparser.statement.select.ValuesList): Unit = ???

  def visit(lateralSubSelect: net.sf.jsqlparser.statement.select.LateralSubSelect): Unit = ???

  def getOperator: Operator = oper
}
