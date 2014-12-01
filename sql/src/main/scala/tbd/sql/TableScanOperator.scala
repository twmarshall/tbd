package tbd.sql

import net.sf.jsqlparser.schema.Table;

class TableScanOperator (val tableDef: net.sf.jsqlparser.schema.Table,
                        val tablesMap: Map[String, ScalaTable]) 
      extends Operator{
  val table = tablesMap.get(tableDef.getName()).get
  val inputAdjustable = table.input.getAdjustableList
  var outputAdjustable = inputAdjustable
  
  var childOperators = List[Operator]();
 
  //override def initialize() {}
  
  override def processOp () = {}  
  
  override def getTable: ScalaTable =  table
  
  override def getAdjustable: tbd.list.AdjustableList[Int,Seq[tbd.sql.Datum]] = outputAdjustable
  
  override def toBuffer = outputAdjustable.toBuffer(table.mutator).map(_._2.map(BufferUtils.getValue))
}