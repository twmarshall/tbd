package tbd.sql

import scala.reflect.runtime.{ universe => ru}
import tbd.{Adjustable, Context, Mod, Mutator}
import tbd.datastore.IntData
import tbd.list.{ListConf, ListInput}
import tbd.TBD._

case class ScalaColumn (val columnName: String, val index: Int, val colType: ru.Type) 
	//extends  net.sf.jsqlparser.schema.Column 

class ScalaTable (val rows: Iterator[Seq[Datum]], val colnameMap: Map[String, ScalaColumn], val table: net.sf.jsqlparser.schema.Table,
				val listConf: ListConf) {
				//extends net.sf.jsqlparser.schema.Table {
  val mutator = new Mutator()
  val input = mutator.createList[Int, Seq[Datum]](listConf)
  rows.zipWithIndex.foreach( row => {
    println(row)
    input.put(row._2, row._1)
  })
  println("added rows to input")
  def printTable = {
    print(input.getAdjustableList.toBuffer(mutator))
  }
  
  /*
  def this() = this(new ListConf(partitions = 1, chunkSize = 1))
  
  def add (row: Seq[Any]) {
  	input.put(key, row)
  	key ++;
  }
  */

}