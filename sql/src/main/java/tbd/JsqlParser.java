package tbd.sql;

import java.io.StringReader;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.Select;


public class JsqlParser {

		static public void main(String[] args) {
			CCJSqlParserManager pm = new CCJSqlParserManager();
			String sql = "SELECT * FROM MY_TABLE1, MY_TABLE2, (SELECT * FROM MY_TABLE3) LEFT OUTER JOIN MY_TABLE4 "+
			" WHERE ID = (SELECT MAX(ID) FROM MY_TABLE5) AND ID2 IN (SELECT * FROM MY_TABLE6)" ;
			//String sql = "SELECT name from People where age> 10";
			net.sf.jsqlparser.statement.Statement statement;
			try {
				statement = pm.parse(new StringReader(sql));
				/* 
				now you should use a class that implements StatementVisitor to decide what to do
				based on the kind of the statement, that is SELECT or INSERT etc. but here we are only
				interested in SELECTS
				*/
				if (statement instanceof Select) {
					Select select = (Select) statement;
					System.out.println(select.toString());
					
				}
			} catch (JSQLParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
}
