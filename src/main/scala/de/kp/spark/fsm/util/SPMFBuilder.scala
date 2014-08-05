package de.kp.spark.fsm.util
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-FSM project
* (https://github.com/skrusche63/spark-fsm).
* 
* Spark-FSM is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-FSM is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-FSM. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object SPMFBuilder {
  
  def build(sc:SparkContext,input:String,format:String, output:Option[String] = None):Option[RDD[String]] = {
  
    val file = format match {
        
      case "BMS" => Some(fromBMS(sc,input))
        
      case "CSV" => Some(fromCSV(sc,input))
       
      case "KOSARAK" => Some(fromKosarak(sc,input))
        
      case "SNAKE" => Some(fromSnake(sc,input))
      
      case "SPMF" => Some(fromSPMF(sc,input))

      case _ => None
      
    }

    file match {
      
      case None => {}
    
      case Some(file) => {

        output match {
      
          case None => {}      
          case Some(output) => save(file, output)
      
        }
        
      }
    
    }
    
    file
    
  }
  
  private def fromBMS(sc:SparkContext,input:String):RDD[String] = {
    
    val source = sc.textFile(input).map(valu => {
          
      val parts = valu.split("\t")  
          
      val uid = Integer.parseInt(parts(0).trim())
      val pid = Integer.parseInt(parts(1).trim())
       
      (uid,pid)
          
    }).groupBy(valu => valu._1).map(valu => {
      /**
       * For further processing the user is ignored here
       */
      val (uid,items) = valu
      var seq:String = ""
        
      for (item <- items) seq += item  + " -1 "
      seq += "-2"
      
      seq
      
    })
  
    index(sc,source)
    
  }      

  private def fromCSV(sc:SparkContext,input:String):RDD[String] = {

    val source = sc.textFile(input).map(valu => {
          
      val sb = new StringBuffer()
      val parts = valu.split(",")  
          
      for (i <- 0 until parts.length) {
            
        val item = Integer.parseInt(parts(i))
        sb.append(item + " -1 ")
      
      }
          
      sb.append("-2")
      sb.toString
      
    })

    index(sc,source)
    
  }

  private def fromKosarak(sc:SparkContext,input:String):RDD[String] = {

    val source = sc.textFile(input).map(valu => {
          
      val sb = new StringBuffer()
      val parts = valu.split(" ")  
          
      for (i <- 0 until parts.length) {
            
        val item = Integer.parseInt(parts(i))
         sb.append(item + " -1 ")
      
      }
          
      sb.append("-2")
      sb.toString
      
    })

    index(sc,source)
	
  }
	
  private def fromSnake(sc:SparkContext,input:String):RDD[String] = {

    val source = sc.textFile(input).map(valu => {
		  
      /**
	   * If the line contains more than 11 elements, we use this 
	   * to filter smaller lines
	   */
      val len = valu.length()
      if (len >= 11) {

        val sb = new StringBuffer()
        /** 
         * for each integer on this line, we consider that it is an item 
         */
		for (i <- 0 until len) {
						
		  /** 
		   * We subtract 65 to get the item number and 
		   * write the item to the file
		   */
		  val character = valu.toCharArray()(i) - 65
		  sb.append(character + " -1 ")
			
		}
			
        sb.append("-2")        
        sb.toString

      } else "#"
      
    }).filter(line => (line.charAt(0) != '#'))

    index(sc,source)

  }

  private def fromSPMF(sc:SparkContext,input:String):RDD[String] = {
    
    val source = sc.textFile(input)
    index(sc,source)
    
  }
  
  private def index(sc:SparkContext,source:RDD[String]):RDD[String] = {
    
    /**
     * Repartition source to single partition
     */
    val file = source.coalesce(1)
    
    val index = sc.parallelize(Range.Long(0, file.count, 1),file.partitions.size)
    val zip = file.zip(index) 
    
    zip.map(valu => {
      
      val (line,no) = valu
      no + "," + line
      
    })
  }
  
  private def save(file:RDD[String], output:String) {
    file.saveAsTextFile(output)
  }
  
}