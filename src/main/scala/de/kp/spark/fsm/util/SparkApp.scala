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

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.serializer.KryoSerializer

class SparkApp {

  protected def createLocalCtx(name:String):SparkContext = {

	System.setProperty("spark.executor.memory", "4g")
	System.setProperty("spark.kryoserializer.buffer.mb","256")
	/**
	 * Other configurations
	 * 
	 * System.setProperty("spark.cores.max", "532")
	 * System.setProperty("spark.default.parallelism", "256")
	 * System.setProperty("spark.akka.frameSize", "1024")
	 * 
	 */	
    val runtime = Runtime.getRuntime()
	runtime.gc()
		
	val cores = runtime.availableProcessors()
		
	val conf = new SparkConf()
	conf.setMaster("local["+cores+"]")
		
	conf.setAppName(name);
    conf.set("spark.serializer", classOf[KryoSerializer].getName)		
        
	new SparkContext(conf)
		
  }

}