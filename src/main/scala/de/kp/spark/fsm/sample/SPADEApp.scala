package de.kp.spark.fsm.sample
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

import de.kp.spark.fsm.util.{SparkApp,SPMFBuilder}
import de.kp.spark.fsm.SPADE

object SPADEApp extends SparkApp {
   
  private val prepare = false
 
  def main(args:Array[String]) {

    val input  = "/Work/tmp/spmf/contextPrefixSpan.txt"
    val output = "/Work/tmp/spmf/contextPrefixSpan"
    
    val result = "/Work/tmp/spmf/result"
      
    var start = System.currentTimeMillis()
    
    val sc = createLocalCtx("SPADEApp")
    
    if (prepare) {
      
      SPMFBuilder.build(sc, input, "SPMF", 4, Some(output))
      println("Prepare Time: " + (System.currentTimeMillis() - start) + " ms")
      
      start = System.currentTimeMillis()
    }

    val support = 0.66 //0.00085
    
    val dfs = true
    val patterns = SPADE.extractFilePatterns(sc, support, dfs)    
 
    /**
     * Serialize pattern
     */
    val serialized = patterns.map(pattern => pattern.serialize())
    
    serialized.foreach(line => println(line))
    println("Total Time: " + (System.currentTimeMillis() - start) + " ms")

  }

}
