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
package tbd.sql;

import java.io.Serializable;
import net.sf.jsqlparser.expression.Expression;

/*
 * A Wrapper of the Column class, such that it can be serializable
 */
public class SerSelectExpressionItem
	extends net.sf.jsqlparser.statement.select.SelectExpressionItem implements Serializable {

	private static final long serialVersionUID = 1L;

	private String alias;

	private Expression expression;
	
	public SerSelectExpressionItem(){}
	
	public SerSelectExpressionItem(net.sf.jsqlparser.statement.select.SelectExpressionItem item) {
		
		super();
		this.setAlias(item.getAlias());
		Expression exp = item.getExpression();
		if (exp instanceof net.sf.jsqlparser.schema.Column) {
			exp = new SerColumn((net.sf.jsqlparser.schema.Column)	exp);
		}
		this.setExpression(exp);
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getAlias() {
		return this.alias;
	}

	public void setExpression(Expression expression) {
		this.expression = expression;
	}

	public Expression getExpression() {
		return this.expression;
	}


}
