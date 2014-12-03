package tbd.sql;

import java.io.Serializable;

import net.sf.jsqlparser.schema.Table;

public class TBDColumn extends net.sf.jsqlparser.schema.Column implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public TBDColumn(Table table, java.lang.String columnName) {
		super(table, columnName);
	}

}
