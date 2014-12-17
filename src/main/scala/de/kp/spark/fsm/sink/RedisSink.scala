package de.kp.spark.fsm.sink
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

import java.util.Date

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.fsm.Configuration
import de.kp.spark.fsm.model._

import scala.collection.JavaConversions._

class RedisSink(host:String,port:Int) extends RedisDB(host,port){

  val service = "fsm"
  
  def addPatterns(req:ServiceRequest, patterns:FSMPatterns) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "pattern:" + service + ":" + req.data("uid")
    val v = "" + timestamp + ":" + Serializer.serializePatterns(patterns)
    
    getClient.zadd(k,timestamp,v)
    
  }
 
  def patternsExist(uid:String):Boolean = {

    val k = "pattern:" + service + ":" + uid
    getClient.exists(k)
    
  }
  
  def patterns(uid:String):String = {

    val k = "pattern:" + service + ":" + uid
    val patterns = getClient.zrange(k, 0, -1)

    if (patterns.size() == 0) {
      Serializer.serializePatterns(new FSMPatterns(List.empty[FSMPattern]))
    
    } else {
      
      val last = patterns.toList.last
      last.split(":")(1)
      
    }
  
  }

}