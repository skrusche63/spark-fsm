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

import de.kp.spark.core.model._
import de.kp.spark.core.io.ElasticWriter

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.io.{ElasticBuilderFactory => EBF}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class ElasticSink {

  def addPatterns(req:ServiceRequest, patterns:FSMPatterns) {
    /*
     * Not implemented yet
     */
  }
  
  def addRules(req:ServiceRequest, rules:FSMRules) {
 
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
    
    val now = new Date()
    val timestamp = now.getTime()
   
    for (rule <- rules.items) {

      /* 
       * Unique identifier to group all entries 
       * that refer to the same rule
       */      
      val rid = UUID.randomUUID().toString()
      val source = new java.util.HashMap[String,Object]()    
      
      source += EBF.TIMESTAMP_FIELD -> timestamp.asInstanceOf[Object]
      source += EBF.UID_FIELD -> uid
      
      source += EBF.RULE_FIELD -> rid
        
      source += EBF.ANTECEDENT_FIELD -> rule.antecedent
      source += EBF.CONSEQUENT_FIELD -> rule.consequent
        
      source += EBF.SUPPORT_FIELD -> rule.support.asInstanceOf[Object]
      source += EBF.CONFIDENCE_FIELD -> rule.confidence.asInstanceOf[Object]
        
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