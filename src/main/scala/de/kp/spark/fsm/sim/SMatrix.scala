package de.kp.spark.fsm.sim
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

import scala.collection.mutable.{ArrayBuffer,ArrayBuilder}

/**
 * SMatrix represents an optimized data structure for
 * a quadratic and symmetric matrix, where the diagonal
 * values are fixed
 */
class SMatrix(dim:Int,diag:Double) extends Serializable {

  protected val table = Array.tabulate(dim)(row => Array.fill[Double](dim-row)(0))

  /**
   * This method requires `row`  < `col` values 
   */
  def set(row:Int,col:Int,valu:Double) {

    val x = row
    val y = col - (x+1)

    table(x)(y) = valu
  
  }
	
  def get(row:Int,col:Int):Double = {    
 
    if (row == col) {
      diag
    
    } else if (row < col) {
      
      val x = row
      val y = col - (x+1)
      
       table(x)(y)
      
    } else {
      
      val x = col
      val y = row - (x+1)

      table(x)(y)
      
    }

  }

  /**
   * Returns the K-means cost of a given point against the given cluster centers.
   * The `cost` is defined as 1 - sim
   */
  def pointCost(centers:Array[Int],point:Int): Double = {
    1 - findClosest(centers, point)._2
  }

  def findCosts(points:Array[Int]):Array[Double] = {
    
    val count = points.size    
    val costs = Array.fill[Double](count)(0)
   
    (0 until count).foreach(i => {

      val row = points(i)
      (0 until count).foreach(j => {

        val col = points(j)
        /*
         * Calculate mean value of cost for a certain point
         * for all points in the cluster
         */
        costs(i) = points.map(point => 1 - get(row,col)).sum / count            
          
      })
        
    })

    costs
    
  }
  
  /**
   * Returns the index of the closest center to the given itemset, as well as the
   * similarity to this center
   */
  def findClosest(centers:Array[Int], point:Int): (Int, Double) = {
    
    val similarities = centers.map(center => get(center,point)).toSeq
    
    val max = similarities.max
    val pos = similarities.indexOf(max)

    (pos, max)
  
  }
  
  def serialize():ArrayBuffer[String] = {
		
    val output = ArrayBuffer.empty[String]		
    (0 until dim).foreach(row => output += serializeRow(row))
	
    output
	
  }
  
  def deserializeRow(data:String) {
    
    val Array(sid,seq) = data.split("\\|")   
    table(sid.toInt) = seq.split(",").map(_.toDouble)
    
  }
  
  def serializeRow(row:Int):String = "" + row + "|" + table(row).mkString(",")

}