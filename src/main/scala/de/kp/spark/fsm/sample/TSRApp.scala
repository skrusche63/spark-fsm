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
import de.kp.spark.fsm.TSR

object TSRApp extends SparkApp {
  
  private val prepare = false
  
  def main(args:Array[String]) {

    val input  = "/Work/tmp/spmf/BMS1_spmf.txt"
    val output = "/Work/tmp/spmf/BMS1_spmf"
    
    var start = System.currentTimeMillis()
    
    val sc = createLocalCtx("TSRApp")
    
    if (prepare) {
      
      SPMFBuilder.build(sc, input, "SPMF", 1000, Some(output))
      println("Prepare Time: " + (System.currentTimeMillis() - start) + " ms")
      
      start = System.currentTimeMillis()
      
    }
    
    val k = 3
    val minconf = 0.8
    
    val rules = TSR.extractRules(sc, output, k, minconf)   
    rules.foreach(rule => {
		
      val sb = new StringBuffer()
	  
      sb.append(rule.toString())
      
	  sb.append(" #SUP: ")
      sb.append(rule.getAbsoluteSupport())
		
      sb.append(" #CONF: ")
	  sb.append(rule.getConfidence());

      println(sb.toString)
      
    })

    val end = System.currentTimeMillis()
    println("==================================")
    println("Total time: " + (end-start) + " ms")
    
  }

}