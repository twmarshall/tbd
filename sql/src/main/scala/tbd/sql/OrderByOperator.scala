package tbd.sql

import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.statement.select.OrderByElement
import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map
import scala.util.control.Breaks._

import tbd._
import tbd.list._

class MergeSortAdjust (list: AdjustableList[Int, Seq[Datum]], 
                       elements: List[_], 
                       var isTupleMapPresent: Boolean)
  extends Adjustable[AdjustableList[Int, Seq[Datum]]] {
  
  def run(implicit c: Context) = {  
    list.mergesort((pair1: (Int, Seq[Datum]), pair2:(Int, Seq[Datum])) => {
      if (isTupleMapPresent) {
        TupleStruct.setTupleTableMap(pair1._2.toArray)
        isTupleMapPresent = false;
      }
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

          cmp = if (op1.isInstanceOf[Long]) 
              op1.asInstanceOf[Long].compare(op2.asInstanceOf[Long])
            else if (op1.isInstanceOf[Double]) 
              op1.asInstanceOf[Double].compare(op2.asInstanceOf[Double])
            else if (op1.isInstanceOf[java.util.Date]) 
              op1.asInstanceOf[java.util.Date].compareTo(op2.asInstanceOf[java.util.Date])
            else 
              op1.asInstanceOf[String].compare(op2.asInstanceOf[String])
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
  var inputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  var outputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  //def initialize() {}
  
  override def processOp () {
    childOperators.foreach(child => child.processOp)
  
    inputAdjustable = inputOper.getAdjustable
    val adjustable = new MergeSortAdjust(inputAdjustable, elements, true)
    outputAdjustable = table.mutator.run(adjustable)
  }
  
  override def getTable: ScalaTable = table
  
  override def getAdjustable: tbd.list.AdjustableList[Int,Seq[tbd.sql.Datum]] = outputAdjustable
  
  override def toBuffer = outputAdjustable.toBuffer(table.mutator).map(_._2.map(BufferUtils.getValue))
}