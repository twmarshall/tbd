package tbd.sql

import scala.io.Source
import tbd.list.ListConf
import tbd.table.TableInput
import tbd.{Adjustable, Context, Mod, Mutator}


class TBDSqlContext[T](listConf: ListConf) {
  
  val mutator = new Mutator()
  val input = TableInput[Int, T](mutator)
  
  def this() = this(new ListConf(partitions = 1, chunkSize = 1))
  
  def csvFile (path: String, f: Array[String] => T): TableInput[Int, T] = {
    val fileContents = Source.fromFile(path).getLines.map(_.split(",")).map(f)
	var i = 0
    for (row <- fileContents) {
	  input.put(i, row)
	  i += 1
	}
    input
  }
}