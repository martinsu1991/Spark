import au.com.bytecode.opencsv.CSVReader

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkFiles
import scala.tools.nsc.Properties
import java.io.StringReader


object LoadCSV {
  def main(args: Array[String]) {

    val inputFile = "/Users/yuhansu/Documents/temp.csv"
    val sc = new SparkContext("local", "LoadCSV")

    val invalidLineCounter = sc.accumulator(0)
    val invalidNumericLineCounter = sc.accumulator(0)

    sc.addFile(inputFile) //copy data into every node
    val inFile = sc.textFile(inputFile)


     /* val splitLines = inFile.map(line => {
        val reader = new CSVReader(new StringReader(line))
        reader.readNext()
      })*/

    val splitLines = inFile.flatMap(line => {
      try
      {
        val reader = new CSVReader(new StringReader(line))
        Some(reader.readNext())      //return valid data
      }
      catch
      {
        case _ =>
        {
          invalidLineCounter += 1     //invalid data counter
          None                        //return invalid
        }

      }

    })

   // val numericData = splitLines.map(line => line.map(_.toDouble))  //every row is a RDD
    val numericData = splitLines.flatMap(line =>
      {
        try
        {
          Some(line.map(_.toDouble))
        }
        catch
        {
          case _ =>
          {
            invalidNumericLineCounter += 1   //invalid data counter
            None                           //return invalid
          }

        }

      }

   )

    println(numericData.collect().mkString(","))
    //numericData.collect().apply(1).foreach(println)
    val summedData =  numericData.map(row => row.sum)
    println(summedData.collect().mkString(","))
    println("Errors:" + invalidLineCounter + "," + invalidNumericLineCounter)

  }
}