package de.kp.spark.fsm.actor
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.source.SequenceSource
import de.kp.spark.core.source.handler.SPMFHandler

import de.kp.spark.fsm.{RequestContext,TSR}

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.sink._

import de.kp.spark.fsm.spec.SequenceSpec
import scala.collection.mutable.ArrayBuffer

class TSRActor(@transient ctx:RequestContext) extends TrainActor(ctx) {
  
  override def train(req:ServiceRequest) {
         
    val source = new SequenceSource(ctx.sc,ctx.config,new SequenceSpec(req))
    val dataset = SPMFHandler.sequence2SPMF(source.connect(req))
      
    val params = ArrayBuffer.empty[Param]
          
    val k = req.data("k").toInt
    params += Param("k","integer",k.toString)
    
    val minconf = req.data("minconf").toDouble
    params += Param("minconf","double",minconf.toString)

    cache.addParams(req, params.toList)

    val total = ctx.sc.broadcast(dataset.count())
    val rules = TSR.extractRDDRules(dataset,k,minconf).map(rule => {
     
      val antecedent = rule.getItemset1().toList
      val consequent = rule.getItemset2().toList

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new Rule(antecedent,consequent,support,total.value,confidence)
            
    })
          
    saveRules(req,new Rules(rules))

  }  
  
  private def saveRules(req:ServiceRequest,rules:Rules) {
    
    redis.addRules(req,rules)
    
    if (req.data.contains(Names.REQ_SINK) == false) return
    
    val sink = req.data(Names.REQ_SINK)
    if (Sinks.isSink(sink) == false) return
    
    sink match {
      
      case Sinks.ELASTIC => {
    
        val elastic = new ElasticSink()
        elastic.addRules(req,rules)
        
      }
      
      case Sinks.JDBC => {
            
        val jdbc = new JdbcSink()
        jdbc.addRules(req,rules)
        

      }
      
      case _ => {/* do nothing */}
      
    }
    
  }
  
  
  override def validate(req:ServiceRequest) = {

    if (req.data.contains("name") == false) 
      throw new Exception("No name for sequential rules provided.")

    if (req.data.contains("k") == false)
      throw new Exception("Parameter 'k' is missing.")
    
    if (req.data.contains("minconf") == false)
      throw new Exception("Parameter 'minconf' is missing.")
    
  }
  
}