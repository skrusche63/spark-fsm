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

import java.util.{Date,UUID}

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.io.ElasticWriter

import de.kp.spark.fsm.model._

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class ElasticSink {

  def addPatterns(req:ServiceRequest, patterns:Patterns) {
    /*
     * Not implemented yet
     */
  }
  
  def addRules(req:ServiceRequest, rules:Rules) {
 
    val uid = req.data("uid")
    /*
     * Elasticsearch is used as a source and also as a sink; this implies
     * that the respective index and mapping must be distinguished
     */    
    val index   = req.data("dst.index")
    val mapping = req.data("dst.type")
    
    val writer = new ElasticWriter()
    
    val readyToWrite = writer.open(index,mapping)
    if (readyToWrite == false) {
      
      writer.close()
      
      val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
      throw new Exception(msg)
      
    }
   
    /*
     * Determine timestamp for the actual set of rules to be indexed
     */
    val now = new Date()
    val timestamp = now.getTime()
   
    for (rule <- rules.items) {

      val source = new java.util.HashMap[String,Object]()    
      
      source += Names.TIMESTAMP_FIELD -> timestamp.asInstanceOf[Object]
      source += Names.UID_FIELD -> uid
        
      source += Names.ANTECEDENT_FIELD -> rule.antecedent
      source += Names.CONSEQUENT_FIELD -> rule.consequent
        
      source += Names.SUPPORT_FIELD -> rule.support.asInstanceOf[Object]
      source += Names.CONFIDENCE_FIELD -> rule.confidence.asInstanceOf[Object]

      source += Names.TOTAL_FIELD -> rule.total.asInstanceOf[Object]
       
      /*
       * Writing this source to the respective index throws an
       * exception in case of an error; note, that the writer is
       * automatically closed 
       */
      writer.write(index, mapping, source)
      
    }
    
    writer.close()
    
  }
  
}