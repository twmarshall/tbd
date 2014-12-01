package tbd.sql

import net.sf.jsqlparser.expression._
import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map

import tbd._
import tbd.datastore.IntData
import tbd.list._


class FilterAdjust(list: AdjustableList[Int, Seq[Datum]], 
					val condition: Expression, 
					var isTupleMapPresent: Boolean)
  extends Adjustable[AdjustableList[Int, Seq[Datum]]] {

  def run (implicit c: Context) = {
    list.filter(pair => {
      if (isTupleMapPresent) {
        TupleStruct.setTupleTableMap(pair._2.toArray)
        isTupleMapPresent = false;
      }
      val eval = new Evaluator(pair._2.toArray)
      condition.accept(eval)
      eval.getAccumulatorBoolean
    })
  }
}

class FilterOperator (val inputOper: Operator, val condition: Expression) 
	extends Operator{
  var childOperators = List[Operator]():+ inputOper;
  val table = inputOper.getTable
  var inputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  var outputAdjustable : AdjustableList[Int,Seq[tbd.sql.Datum]] = _
  var isTupleMapPresent = true
  //def initialize() {}
  
  override def processOp () {
    childOperators.foreach(child => child.processOp)
    
    inputAdjustable = inputOper.getAdjustable
    val adjustable = new FilterAdjust(inputAdjustable, condition, isTupleMapPresent)
    outputAdjustable = table.mutator.run(adjustable)
  }
  
  override def getTable: ScalaTable = table
  
  override def getAdjustable: tbd.list.AdjustableList[Int,Seq[tbd.sql.Datum]] = outputAdjustable
  
  override def toBuffer = outputAdjustable.toBuffer(table.mutator).map(_._2.map(BufferUtils.getValue))
}