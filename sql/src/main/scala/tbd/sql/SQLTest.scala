package tbd.sql
import tbd.{Adjustable, Context, Mod, Mutator}
import tbd.list.ListConf
import tbd.master.{Master, MasterConnector}

case class Rec (val manipulate: String, val pairkey: Int, val pairvalue: Double)

object SQLTest {
  def main (args: Array[String]) {
    val connector = MasterConnector("akka.tcp://masterSystem0@128.237.217.40:2552/user/master")
    
    val listConf =  new ListConf(partitions = 2, chunkSize = 1)
    val mutator = new Mutator(connector)
    val sqlContext = new TBDSqlContext(mutator)
    var f = (row: Array[String])  => Rec(row(0), row(1).trim.toInt, row(2).trim.toDouble)
    val tableName1 = "records1"
    val tableName2 = "records2"
    val path = "sql/data.csv"
    
    sqlContext.csvFile[Rec](tableName1, path, f, listConf)
    sqlContext.csvFile[Rec](tableName2, path, f)
    val statement = "select  * from records1 "
    val oper = sqlContext.sql(statement)
    println(oper.toBuffer)
    mutator.shutdown()
  }
}