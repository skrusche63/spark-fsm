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

class TSRActor extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("TSRActor",Configuration.spark)      

  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")     
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == 0.0))

      if (params != null) {
        /* Register status */
        JobCache.add(uid,FSMStatus.STARTED)
 
        try {
          
          val source = req.data("source")
          val dataset = source match {
            
            /* 
             * Discover top k sequence rules from sequence database persisted 
             * as an appropriate search index from Elasticsearch; the configuration
             * parameters are retrieved from the service configuration 
             */    
            case Sources.ELASTIC => new ElasticSource(sc).connect()
            /* 
             * Discover top k sequence rules from sequence database persisted 
             * as a file on the (HDFS) file system; the configuration parameters are 
             * retrieved from the service configuration  
             */    
            case Sources.FILE => new FileSource(sc).connect()
            /*
             * Retrieve Top-K sequence rules from sequence database persisted 
             * as an appropriate table from a JDBC database; the configuration parameters 
             * are retrieved from the service configuration
             */
            //case Sources.JDBC => new JdbcSource(sc).connect(req.data)
             /*
             * Retrieve Top-K sequence rules from sequence database persisted 
             * as an appropriate table from a Piwik database; the configuration parameters 
             * are retrieved from the service configuration
             */
            //case Sources.PIWIK => new PiwikSource(sc).connect(req.data)
            
          }

          JobCache.add(uid,FSMStatus.DATASET)
          
          val (k,minconf) = params     
          findRules(uid,dataset,k,minconf)

        } catch {
          case e:Exception => JobCache.add(uid,FSMStatus.FAILURE)          
        }
 

      }
      
      sc.stop
      context.stop(self)
          
    }
    
    case _ => {
      
      sc.stop
      context.stop(self)
      
    }
    
  }
  
  private def findRules(uid:String,dataset:RDD[(Int,String)],k:Int,minconf:Double) {
     
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