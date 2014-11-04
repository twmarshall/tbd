package tbd.sql

abstract class Operator {
  var childOperators = List[Operator]();
  var parentOperators = List[Operator]();
  
  def initialize() {}
  
  def processOp (row: Any) {}
}