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

import akka.actor.{Actor,ActorLogging}

import de.kp.spark.fsm.{Configuration,SPADE}
import de.kp.spark.fsm.source.SequenceSource

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.redis.RedisCache

import de.kp.spark.fsm.sink.RedisSink

class SPADEActor(@transient val sc:SparkContext) extends Actor with ActorLogging {

  def receive = {
    
    case req:ServiceRequest => {
      
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == 0.0))

      if (params != null) {
        /* Register status */
        RedisCache.addStatus(req,FSMStatus.STARTED)
 
        try {
          
          val dataset = new SequenceSource(sc).get(req.data)
 
          RedisCache.addStatus(req,FSMStatus.DATASET)
          
          val support = params     
          findPatterns(req,dataset,support)

        } catch {
          case e:Exception => RedisCache.addStatus(req,FSMStatus.FAILURE)          
        }
 

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def findPatterns(req:ServiceRequest,dataset:RDD[(Int,String)],support:Double) {
     
    val patterns = SPADE.extractRDDPatterns(dataset,support).map(pattern => {
      
      val line = pattern.serialize()
      // 1 -1 3 -1 3 -1 | 3
      val Array(sequence,cardinality) = line.split("\\|")
      
      val support = cardinality.trim().toInt
      val itemsets = sequence.trim().split("-1").map(itemset => itemset.trim().split(" ").map(_.toInt).toList).toList

      new FSMPattern(support,itemsets)
      
    }).toList
          
    savePatterns(req,new FSMPatterns(patterns))
          
    /* Update status */
    RedisCache.addStatus(req,FSMStatus.FINISHED)

  }  
  
  private def savePatterns(req:ServiceRequest,patterns:FSMPatterns) {
    
    val sink = new RedisSink()
    sink.addPatterns(req,patterns)
    
  }
  
  private def properties(req:ServiceRequest):Double = {
      
    try {
   
      val support = req.data("support").toDouble        
      return support
        
    } catch {
      case e:Exception => {
         return 0.0          
      }
    }
    
  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,FSMStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.MINING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,FSMStatus.STARTED)	
  
    }

  }
  
}