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

import net.sf.jsqlparser.schema.Table;

/*
 * A Wrapper of the Table class, such that it can be serializable
 */
public class SerTable extends net.sf.jsqlparser.schema.Table
	implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public SerTable() {}
	
	public SerTable(java.lang.String schemaName, java.lang.String name) {
		super(schemaName, name);
	}
	
	public SerTable(net.sf.jsqlparser.schema.Table table) {
		super(table.getSchemaName(), table.getName());
	}
}