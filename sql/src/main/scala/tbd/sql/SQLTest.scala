package tbd.sql

case class Rec (val manipulate: String, val pairkey: Int, val pairvalue: Double)

object SQLTest {
  def main (args: Array[String]) {
    
    val sqlContext = new TBDSqlContext()
    var f = (row: Array[String])  => Rec(row(0), row(1).trim.toInt, row(2).trim.toDouble)
    val tableName1 = "records1"
    val tableName2 = "records2"
    val path = "sql/data.csv"
    
    sqlContext.csvFile[Rec](tableName1, path, f)
    sqlContext.csvFile[Rec](tableName2, path, f)
    val statement = "select   * from records1 "
    val oper = sqlContext.sql(statement)
    println(oper.asInstanceOf[ProjectionOperator].toBuffer)
  }
}