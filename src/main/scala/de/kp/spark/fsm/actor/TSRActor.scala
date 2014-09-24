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

import akka.actor.Actor

import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.{Configuration => HConf}

import de.kp.spark.fsm.{Configuration,FSMRule,TSR}
import de.kp.spark.fsm.source.{ElasticSource,FileSource}

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.util.{JobCache,RuleCache}

import scala.collection.JavaConversions._

class TSRActor(jobConf:JobConf) extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("TSRActor",Configuration.spark)      
  
  private val uid = jobConf.get("uid").get.asInstanceOf[String]     
  JobCache.add(uid,FSMStatus.STARTED)

  private val params = parameters()

  private val response = if (params == null) {
    val message = FSMMessages.MISSING_PARAMETERS(uid)
    new FSMResponse(uid,Some(message),None,None,None,FSMStatus.FAILURE)
  
  } else {
     val message = FSMMessages.MINING_STARTED(uid)
     new FSMResponse(uid,Some(message),None,None,None,FSMStatus.STARTED)
    
  }

  def receive = {
    /*
     * Retrieve Top-K sequence rules from an appropriate index from Elasticsearch
     */     
    case req:ElasticRequest => {

      /* Send response to originator of request */
      sender ! response
          
      if (params != null) {

        try {
          
          /* Retrieve data from Elasticsearch */    
          val dataset = new ElasticSource(sc).connect()

          JobCache.add(uid,FSMStatus.DATASET)
          
          val (k,minconf) = params     
          findRules(dataset,k,minconf)

        } catch {
          case e:Exception => JobCache.add(uid,FSMStatus.FAILURE)          
        }
      
      }
      
      sc.stop
      context.stop(self)
      
    }
    
    /*
     * Retrieve Top-K sequence rules from an appropriate file from the
     * (HDFS) file system; the file MUST have a specific file format;
     * 
     * actually it MUST be ensured by the client application that such
     * a file exists in the right format
     */
    case req:FileRequest => {

      /* Send response to originator of request */
      sender ! response
          
      if (params != null) {

        try {
    
          /* Retrieve data from the file system */
          val dataset = new FileSource(sc).connect()

          JobCache.add(uid,FSMStatus.DATASET)

          val (k,minconf) = params          
          findRules(dataset,k,minconf)

        } catch {
          case e:Exception => JobCache.add(uid,FSMStatus.FAILURE)
        }
        
      }
      
      sc.stop
      context.stop(self)
      
    }
    
    case _ => {}
    
  }
  
  private def findRules(dataset:RDD[(Int,String)],k:Int,minconf:Double) {
     
    val rules = TSR.extractRDDRules(dataset,k,minconf).map(rule => {
     
      val antecedent = rule.getItemset1().toList
      val consequent = rule.getItemset2().toList

      val support    = rule.getAbsoluteSupport()
      val confidence = rule.getConfidence()
	
      new FSMRule(antecedent,consequent,support,confidence)
            
    })
          
    /* Put rules to RuleCache */
    RuleCache.add(uid,rules)
          
    /* Update JobCache */
    JobCache.add(uid,FSMStatus.FINISHED)

  }  
  
  private def parameters():(Int,Double) = {
      
    try {
      val k = jobConf.get("k").get.asInstanceOf[Int]
      val minconf = jobConf.get("minconf").get.asInstanceOf[Double]
        
      return (k,minconf)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
}