package tbd.sql.test

import scala.io.Source
import scala.collection.mutable.{ ArrayBuffer, Buffer, Map }
import scala.util.control.Breaks._
import org.scalatest._

import tbd.{ Adjustable, Context, Mod, Mutator }
import tbd.datastore.IntData
import tbd.list._
import tbd.TBD._
import tbd.sql._

class JoinTests extends FlatSpec with Matchers {

  "Join Test" should "check the Join operator" in {
    val sqlContext = new TBDSqlContext()
    val f = (row: Array[String]) => Rec(row(0), row(1).trim.toLong, row(2).trim.toDouble)
    val tableName1 = "records1"
    val tableName2 = "records2"
    val path = "data.csv"

    sqlContext.csvFile[Rec](tableName1, path, f)
    sqlContext.csvFile[Rec](tableName2, path, f)

    var statement = "select  r1.manipulate, r1.pairkey, r1.pairvalue " +
      "from records1 as r1 inner join records2 as r2 " +
      "on r1.manipulate=r2.manipulate where r1.pairkey > r2.pairkey"
    var oper = sqlContext.sql(statement)

    val inputs1 = Source.fromFile(path).getLines.map(_.split(",")).map(f).toList
    val inputs2 = Source.fromFile(path).getLines.map(_.split(",")).map(f).toList

    var inputs = List[Rec]()

    for (input1 <- inputs1;
      input2 <- inputs2;
          if input1.manipulate == input2.manipulate; 
          if input1.pairkey > input2.pairkey ){
            inputs = inputs :+ input1
    }
    oper.toBuffer should be(inputs.map(r => Seq(r.manipulate, r.pairkey, r.pairvalue)).toBuffer)
    
    statement = "select  r1.manipulate, r1.pairkey, r1.pairvalue " +
      "from records1 as r1 inner join records2 as r2 " +
      "on r1.manipulate=r2.manipulate and r1.pairkey > r2.pairkey"
    oper = sqlContext.sql(statement)
    oper.toBuffer should be(inputs.map(r => Seq(r.manipulate, r.pairkey, r.pairvalue)).toBuffer)
    
    statement = "select  r1.manipulate, r1.pairkey, r1.pairvalue " +
      "from records1 as r1 inner join records2 as r2 " +
      "where r1.manipulate=r2.manipulate and r1.pairkey > r2.pairkey"
    oper = sqlContext.sql(statement)
    oper.toBuffer should be(inputs.map(r => Seq(r.manipulate, r.pairkey, r.pairvalue)).toBuffer)

    sqlContext.shutDownMutator
  }
  
  "Join Integrate Test" should "check the integration of Join operator to others" in {
    val sqlContext = new TBDSqlContext()
    val f = (row: Array[String]) => Rec(row(0), row(1).trim.toLong, row(2).trim.toDouble)
    val tableName1 = "records1"
    val tableName2 = "records2"
    val path = "data.csv"

    sqlContext.csvFile[Rec](tableName1, path, f)
    sqlContext.csvFile[Rec](tableName2, path, f)

    var statement = "select  r1.manipulate, count(r1.pairkey), sum(r1.pairvalue) " +
      "from records1 as r1 inner join records2 as r2 " +
      "on r1.manipulate=r2.manipulate where r1.pairkey > r2.pairkey " +
      "group by r1.manipulate"
    var oper = sqlContext.sql(statement)

    val inputs1 = Source.fromFile(path).getLines.map(_.split(",")).map(f).toList
    val inputs2 = Source.fromFile(path).getLines.map(_.split(",")).map(f).toList

    var inputs = List[Rec]()

    for (input1 <- inputs1;
      input2 <- inputs2;
          if input1.manipulate == input2.manipulate; 
          if input1.pairkey > input2.pairkey ){
            inputs = inputs :+ input1
    }
    val outputs =  inputs.groupBy(r => r.manipulate).map(pair => {
      Seq(pair._1, pair._2.length, pair._2.foldLeft(0.0)(_ + _.pairvalue))
    })
    oper.toBuffer should be(outputs.toBuffer.sortWith((r1, r2) => 
      r1(0).asInstanceOf[String].compareTo(r2(0).asInstanceOf[String]) < 0))
    
    sqlContext.shutDownMutator
  }

}
