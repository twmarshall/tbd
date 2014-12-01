package tbd.sql.test

import scala.io.Source
import scala.collection.mutable.{ ArrayBuffer, Buffer, Map }
import org.scalatest._

import tbd.{ Adjustable, Context, Mod, Mutator }
import tbd.datastore.IntData
import tbd.list._
import tbd.TBD._
import tbd.sql._

class OrderByTests extends FlatSpec with Matchers {

  "Orderby Test" should "return the sorted rows" in {
    val sqlContext = new TBDSqlContext()
    val f = (row: Array[String]) => Rec(row(0), row(1).trim.toLong, row(2).trim.toDouble)
    val tableName = "records"
    val path = "data.csv"
    sqlContext.csvFile[Rec](tableName, path, f)

    var statement = "select   2* pairvalue, pairkey from records where pairvalue > 10 order by pairkey"
    var oper = sqlContext.sql(statement)

    var inputs = Source.fromFile(path).getLines.map(_.split(",")).map(f).filter(r =>
      r.pairvalue > 10).map(r => Seq(r.pairvalue * 2, r.pairkey))

    oper.toBuffer should be(inputs.toBuffer.sortWith((r1, r2) => r1(1).asInstanceOf[Long] < r2(1).asInstanceOf[Long]))

    statement = "select   2* pairvalue, pairkey from records where pairvalue > 10 order by pairkey desc"
    oper = sqlContext.sql(statement)

    inputs = Source.fromFile(path).getLines.map(_.split(",")).map(f).filter(r =>
      r.pairvalue > 10).map(r => Seq(r.pairvalue * 2, r.pairkey))

    oper.toBuffer should be(inputs.toBuffer.sortWith((r1, r2) => r1(1).asInstanceOf[Long] > r2(1).asInstanceOf[Long]))

    sqlContext.shutDownMutator
  }

}
