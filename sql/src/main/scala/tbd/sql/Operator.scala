package tbd.sql

object BufferUtils {
  def getValue(d: tbd.sql.Datum) = {
    if (d.isInstanceOf[Datum.dLong]) 
        d.asInstanceOf[Datum.dLong].getValue()
  else if (d.isInstanceOf[Datum.dDecimal]) 
        d.asInstanceOf[Datum.dDecimal].getValue()
    else if (d.isInstanceOf[Datum.dDate]) 
        d.asInstanceOf[Datum.dDate].getValue()
    else d.asInstanceOf[Datum.dString].getValue()    
  }
}

trait Operator {
  
  def initialize() {}
  
  def processOp () {}
  
  def getTable: ScalaTable 
  
  def getAdjustable: tbd.list.AdjustableList[Int,Seq[tbd.sql.Datum]]
  
  def toBuffer: Any
}