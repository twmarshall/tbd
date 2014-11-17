package tbd.sql

trait Operator {
  
  def initialize() {}
  
  def processOp () {}
  
  def getTable: ScalaTable 
  
  def getAdjustable: tbd.list.AdjustableList[Int,Seq[tbd.sql.Datum]]
}