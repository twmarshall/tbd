package tbd.sql

import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map

import tbd._
import tbd.datastore.IntData
import tbd.list._

class GroupByAdjust(list: AdjustableList[Int, Seq[Datum]], val groupbyColIdxList: List[Int],  val projectStmt: List[_])
  extends Adjustable[AdjustableList[Int, Seq[Datum]]] {
  
  def groupByComparator (pair1: (Int, Seq[Datum]), pair2:(Int, Seq[Datum])): Int = {
    var cmp = 0
    for (colIdx <- groupbyColIdxList) {
      cmp = pair1._2(colIdx).compareTo(pair2._2(colIdx))
      if ( cmp != 0) return cmp
    }
    cmp
  }
  
  def reduceFun (row1: Seq[Datum], row2:Seq[Datum]): Seq[Datum] = {
    val t = row2.toArray
    var newDatumList = List[Datum]()
    projectStmt.foreach( item => {
	      val selectItem = item.asInstanceOf[SelectExpressionItem]
	      val sv = new SelectItemParseVisitor
	      selectItem.accept(sv)
	      sv.getItemType match {
	        //case SELECTTYPE.ALLCOLUMNS | SELECTTYPE.ALLTABLECOLUMNS => newDatumList = newDatumList ++ t.toList ;
	        case SELECTTYPE.SELECTEXPRESSIONITEM => {
	          val e = selectItem.asInstanceOf[SelectExpressionItem].getExpression()
	          val eval = new Evaluator(t.toArray)
	          e.accept(eval)
	          val newCol = selectItem.asInstanceOf[SelectExpressionItem].getAlias() match {
	            case null => eval.getColumn()
	            case _ => new net.sf.jsqlparser.schema.Column(null, selectItem.asInstanceOf[SelectExpressionItem].getAlias())
		      }
	          if (e.isInstanceOf[net.sf.jsqlparser.expression.Function]) {
	            // Aggregate function
	            val aggregateFunction = e.asInstanceOf[net.sf.jsqlparser.expression.Function] 
	            val datum = aggregateFunction.getName() match {
	              case "SUM" => eval.getColumn()
	            }
	          } else {
	            // Select expression
	            
		          val ob = eval.getResult()
		          val datum = ob match {
			        case Int | Long => new Datum.dLong(ob.asInstanceOf[Long], newCol)
			        case Double | Float => new Datum.dDecimal(ob.asInstanceOf[Double], newCol)
			        case d: java.util.Date  => new Datum.dDate(ob.asInstanceOf[java.util.Date], newCol)
			        case _ => new Datum.dString(ob.toString, newCol)
			      }  
	          newDatumList = newDatumList :+ datum
	          }
	        } //end case SELECTEXPRESSIONITEM
	      } //end match sv.getItemType
    	})
    val row = Seq[Datum]()
    row
  }

  def run (implicit c: Context) = {
    list
    /*
    list.reduceByKey(_ + _, (pair1: (Int, Seq[Datum]), pair2:(Int, Seq[Datum])) => {
    	val t = pair._2.toArray
    	var newDatumList = List[Datum]()
    
    	projectStmt.foreach( item => {
	      val selectItem = item.asInstanceOf[SelectExpressionItem]
	      val sv = new SelectItemParseVisitor
	      selectItem.accept(sv)
	      sv.getItemType match {
	        case SELECTTYPE.ALLCOLUMNS | SELECTTYPE.ALLTABLECOLUMNS => newDatumList = newDatumList ++ t.toList ;
	        case SELECTTYPE.SELECTEXPRESSIONITEM => {
	          val e = selectItem.asInstanceOf[SelectExpressionItem].getExpression()
	          val eval = new Evaluator(t.toArray)
	          e.accept(eval)
	          val newCol = selectItem.asInstanceOf[SelectExpressionItem].getAlias() match {
	            case null => eval.getColumn()
	            case _ => new net.sf.jsqlparser.schema.Column(null, selectItem.asInstanceOf[SelectExpressionItem].getAlias())
	          }
	          val ob = eval.getResult()
	          val datum = ob match {
		        case Int | Long => new Datum.dLong(ob.asInstanceOf[Long], newCol)
		        case Double | Float => new Datum.dDecimal(ob.asInstanceOf[Double], newCol)
		        case d: java.util.Date  => new Datum.dDate(ob.asInstanceOf[java.util.Date], newCol)
		        case _ => new Datum.dString(ob.toString, newCol)
		      }  
	          newDatumList = newDatumList :+ datum
	        } //end case SELECTEXPRESSIONITEM
	      } //end match sv.getItemType
    	})
      (pair._1, newDatumList.toSeq)
    })
    * *
    */
  }
  
  
}

class GroupByOperator (val inputOper: Operator, val groupbyList: List[Column], val selectExpressionList: List[SelectExpressionItem]  ) 
	extends Operator{
  var childOperators = List[Operator]():+ inputOper;
  val table = inputOper.getTable
  val inputAdjustable = inputOper.getAdjustable
  var outputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  //def initialize() {}
  
  override def processOp () {
    childOperators.foreach(child => child.processOp)
    var groupbyColIdxList = groupbyList.map(col => table.colnameMap.get(col.getColumnName()).get.index)
    
    
  }
  
  override def getTable: ScalaTable = table
  
  override def getAdjustable: tbd.list.AdjustableList[Int,Seq[tbd.sql.Datum]] = outputAdjustable
}