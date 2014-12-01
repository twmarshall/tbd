package tbd.sql.test

import scala.io.Source
import scala.collection.mutable.{ ArrayBuffer, Buffer, Map }
import org.scalatest._

import tbd.{ Adjustable, Context, Mod, Mutator }
import tbd.datastore.IntData
import tbd.list._
import tbd.TBD._
import tbd.sql._

class AggregateGroupByTests extends FlatSpec with Matchers {

  "Aggregate and GroupBy Test" should "check the GroupBy operator" in {
    val sqlContext = new TBDSqlContext()
    val f = (row: Array[String]) => Rec(row(0), row(1).trim.toLong, row(2).trim.toDouble)
    val tableName = "records"
    val path = "data.csv"
    sqlContext.csvFile[Rec](tableName, path, f)

    var statement = "select  manipulate, count(pairkey), max(pairkey), min(pairkey), sum(pairvalue),  avg(pairvalue) " +
    			    "from records where pairvalue > 10 group by manipulate"
    var oper = sqlContext.sql(statement)

    val reduceFunc = (pair: (String, List[Rec])) => {
      Seq(pair._1, pair._2.length, 
          pair._2.foldLeft(Long.MinValue)(_ max _.pairkey),
          pair._2.foldLeft(Long.MaxValue)(_ min _.pairkey),
          
          pair._2.foldLeft(0.0)(_ + _.pairvalue),
          pair._2.foldLeft(0.0)(_ + _.pairvalue) / pair._2.length
          )
    }
    var inputs = Source.fromFile(path).getLines.map(_.split(",")).map(f).filter(r =>
      r.pairvalue > 10).toList.groupBy(r=>r.manipulate).map(reduceFunc)

    oper.toBuffer should be(inputs.toBuffer.sortWith((r1, r2) => 
      r1(0).asInstanceOf[String].compareTo(r2(0).asInstanceOf[String]) < 0))


    sqlContext.shutDownMutator
  }

}
