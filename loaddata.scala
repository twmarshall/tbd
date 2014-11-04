import scala.io.Source
case class Record(manipulate: String, pair: String) 
object loaddata {
	def main(args: Array[String]) {
		
		val filename = "data.txt"

		for (line <- Source.fromFile(filename).getLines()) {
		  println(line)
		}
		val fileContents = Source.fromFile(filename).getLines.map(_.split(","))//.map(p => Record(p(0),p(1)))
		
		
		val f = (p: Array[String]) => (Record(p(0), p(1)))
		mapper[Record] (fileContents, f)
		

	}

	def mapper[T] (fileContents: Iterator[Array[String]], f: Array[String] => T) = {
		
		for (line <- fileContents.map(f)) {
		  println(line)
		}
	}

}