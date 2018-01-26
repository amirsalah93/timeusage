package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import timeusage.TimeUsage.spark

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

  test("dfSchema "){
    import org.apache.spark.sql.types._
    val columnNames:List[String] = List("foo", "bar", "baz")
    val resdf = TimeUsage.dfSchema(columnNames)

    val resp =  StructType(Array(StructField("foo", StringType, false),
                                                      StructField("bar", DoubleType, false),
                                                      StructField("baz", DoubleType, false)))
    val res = resdf === resp
    assert(res,"\n not equal to StringType(StructField(foo, StringType, false),StructField(bar, DoubleType, false),StructField(bar, DoubleType, false))")
  }

test("row"){
  import spark.implicits._
  val line:List[String]= List("aa", "1.0", "2")
  val rowexp =  Row(line(0), line(1).toDouble, line(2).toDouble)
  val row = TimeUsage.row(line)

  assert(row===rowexp, row + " does not much "+ rowexp)
}


}
