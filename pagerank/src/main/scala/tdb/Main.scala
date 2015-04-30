package tdb.pagerank

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.StreamingContext._

object Main {
  def main(args: Array[String]) {
    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("pagerank").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("/tmp")
    val input = ssc.socketTextStream("localhost", 9999)

    val edges = input.map {
      case line =>
        val split = line.split("\\W+")
        (split(0).toInt, split(1).toInt)
    }

    val adjacency = edges.updateStateByKey((newEdges: Seq[Int], currentEdges: Option[Iterable[Int]]) => {
      if (!currentEdges.isEmpty) {
        Option(currentEdges.get ++ newEdges)
      } else {
        Option(newEdges)
      }
    })

    val contribs = adjacency.map((pair: (Int, Iterable[Int])) => {
      0
    })

    adjacency.foreachRDD { rdd =>
      println("?? " + rdd.count())
      val a = rdd.map { value =>
        println("foreach " + value)
      }
      a.count()
    }

    contribs.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
