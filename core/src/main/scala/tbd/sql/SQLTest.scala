package tbd.sql

case class Rec (val manipulate: String, val pairkey: Int, val pairvalue: Double)

object SQLTest {
  def main (args: Array[String]) {
    
    val sqlContext = new TBDSqlContext[Rec]()
    var f = (row: Array[String])  => Rec(row(0), row(1).trim.toInt, row(2).trim.toDouble)
    val tableName = "records"
    val path = "/Users/apple/git/capstone/tbd/core/data.csv"
    val statement = "select *, pairkey, pairvalue from records where pairkey > 50"
    sqlContext.csvFile(tableName, path, f)
    sqlContext.sql(statement)
  }
}