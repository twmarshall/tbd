package tbd.sql

import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.statement.select.OrderByElement
import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map
import scala.util.control.Breaks._

import tbd._
import tbd.list._

class MergeSortAdjust(list: AdjustableList[Int, Seq[Datum]], elements: List[_])
  extends Adjustable[AdjustableList[Int, Seq[Datum]]] {
  def run(implicit c: Context) = {
    list.mergesort((pair1: (Int, Seq[Datum]), pair2:(Int, Seq[Datum])) => {
      var cmp = 0
      breakable {
         elements.foreach(element => {
	        val elem = element.asInstanceOf[OrderByElement]
	        val exp = elem.getExpression()
	        val isAsc = elem.isAsc()
	        val eval1 = new Evaluator(pair1._2.toArray)
	        exp.accept(eval1)
	        val eval2 = new Evaluator(pair2._2.toArray)
	        exp.accept(eval2)
	        val op1 = eval1.getResult()
	        val op2 = eval2.getResult()
	        cmp = op1 match {
			        case Long | Int => op1.asInstanceOf[Long].compare(op2.asInstanceOf[Long])
			        case Double | Float => op1.asInstanceOf[Double].compare(op2.asInstanceOf[Double])
			        case d: java.util.Date  => op1.asInstanceOf[java.util.Date].compareTo(op2.asInstanceOf[java.util.Date])
			        case _ => op1.asInstanceOf[String].compare(op2.asInstanceOf[String])
			} 
	        if ( ! isAsc) cmp = cmp * -1
	        if (cmp != 0) break
	      })
      }
     
      cmp
    })
  }
}

class OrderByOperator (val inputOper: Operator, val elements: List[_]) 
	extends Operator{
  var childOperators = List[Operator]():+ inputOper;
  val table = inputOper.getTable
  val inputAdjustable = inputOper.getAdjustable
  var outputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  //def initialize() {}
  
  override def processOp () {
    childOperators.foreach(child => child.processOp)
  
    val adjustable = new MergeSortAdjust(table.input.getAdjustableList, elements)
    
  }
  
  override def getTable: ScalaTable = table
  
  override def getAdjustable: tbd.list.AdjustableList[Int,Seq[tbd.sql.Datum]] = outputAdjustable
}