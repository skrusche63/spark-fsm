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

import de.kp.spark.fsm.Configuration
import de.kp.spark.fsm.FSMPattern

import java.util.Date

object PatternCache {
  
  private val maxentries = Configuration.cache  
  private val cache = new LRUCache[(String,Long),List[FSMPattern]](maxentries)

  def add(uid:String,patterns:List[FSMPattern]) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = (uid,timestamp)
    val v = patterns
    
    cache.put(k,v)
    
  }
  
  def exists(uid:String):Boolean = {
    
    val keys = cache.keys().filter(key => key._1 == uid)
    (keys.size > 0)
    
  }
  
  def patterns(uid:String):List[FSMPattern] = {
    
    val keys = cache.keys().filter(key => key._1 == uid)
    if (keys.size == 0) {    
      null
      
    } else {
      
      val last = keys.sortBy(_._2).last
      cache.get(last) match {
        
        case None => null
        case Some(patterns) => patterns
      
      }
      
    }
  
  }

}