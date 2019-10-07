package steve.scala.basic

import scala.collection.mutable.ListBuffer

object ListTest {
  def main(args: Array[String]): Unit = {
    val a: ListBuffer[String] = new ListBuffer[String]
    for (i <- 1 to 10) a.append("hello")
    println(a.length)
  }
}
