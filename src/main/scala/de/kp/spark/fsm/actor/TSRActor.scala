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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.source.SequenceSource

import de.kp.spark.fsm.{Configuration,TSR}

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.sink._

import de.kp.spark.fsm.spec.SequenceSpec
import scala.collection.JavaConversions._

class TSRActor(@transient val sc:SparkContext) extends BaseActor {
  
  private val config = Configuration
  
  private val (host,port) = config.redis
  val redis = new RedisSink(host,port.toInt)

  def receive = {
    
    case req:ServiceRequest => {

      val params = properties(req)
      val missing = (params == null)

      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        cache.addStatus(req,ResponseStatus.MINING_STARTED)
 
        try {
          
          val source = new SequenceSource(sc,config,SequenceSpec)
          val dataset = source.connect(req)
          
          val (k,minconf) = params     
          findRules(req,dataset,k,minconf)

        } catch {
          case e:Exception => cache.addStatus(req,ResponseStatus.FAILURE)          
        }
 

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def findRules(req:ServiceRequest,dataset:RDD[(Int,String)],k:Int,minconf:Double) {
     
    val total = dataset.count()
    val rules = TSR.extractRDDRules(dataset,k,minconf).map(rule => {
     
      val antecedent = rule.getItemset1().toList
      val consequent = rule.getItemset2().toList

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new Rule(antecedent,consequent,support,total,confidence)
            
    })
          
    saveRules(req,new Rules(rules))
          
    /* Update status */
    cache.addStatus(req,ResponseStatus.MINING_FINISHED)

    /* Notify potential listeners */
    notify(req,ResponseStatus.MINING_FINISHED)

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
  
  
  private def properties(req:ServiceRequest):(Int,Double) = {
      
    try {
      
      val k = req.data("k").asInstanceOf[Int]
      val minconf = req.data("minconf").asInstanceOf[Double]
        
      return (k,minconf)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
}