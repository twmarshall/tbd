package tbd.sql

import scala.reflect.runtime.{ universe => ru}
import tbd.{Adjustable, Context, Mod, Mutator}
import tbd.datastore.IntData
import tbd.list.{ListConf, ListInput}
import tbd.TBD._

case class ScalaColumn (val columnName: String, val index: Int, val colType: ru.Type) 
	

class ScalaTable (val rows: Iterator[Seq[Datum]], 
				val colnameMap: Map[String, ScalaColumn], 
				val table: net.sf.jsqlparser.schema.Table,
				val mutator: Mutator,
				val listConf: ListConf) {
	
  
  val input = mutator.createList[Int, Seq[Datum]](listConf)
  rows.zipWithIndex.foreach( row => {  
    input.put(row._2, row._1)
  })
  
  def printTable = {
    print(input.getAdjustableList.toBuffer(mutator))
  }
}