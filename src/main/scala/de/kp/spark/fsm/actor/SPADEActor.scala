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

import de.kp.spark.fsm.{Configuration,FSMPattern,SPADE}
import de.kp.spark.fsm.source.{ElasticSource,FileSource}

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.util.{JobCache,PatternCache}

class SPADEActor(jobConf:JobConf) extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("SPADEActor",Configuration.spark)      
  
  private val uid = jobConf.get("uid").get.asInstanceOf[String]     
  JobCache.add(uid,FSMStatus.STARTED)

  private val params = parameters()

  private val response = if (params == 0.0) {
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
          val source = new ElasticSource(sc)
          val dataset = source.connect()

          JobCache.add(uid,FSMStatus.DATASET)
          
          val support = params     
          findPatterns(dataset,support)

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
          val source = new FileSource(sc)
          
          val path = req.path
          val dataset = source.connect()

          JobCache.add(uid,FSMStatus.DATASET)

          val support = params          
          findPatterns(dataset,support)

        } catch {
          case e:Exception => JobCache.add(uid,FSMStatus.FAILURE)
        }
        
      }
      
      sc.stop
      context.stop(self)
      
    }
    
    case _ => {}
    
  }
  
  private def findPatterns(dataset:RDD[(Int,String)],support:Double) {
     
    val patterns = SPADE.extractRDDPatterns(dataset,support).map(pattern => {
      
      val line = pattern.serialize()
      // 1 -1 3 -1 3 -1 | 3
      val Array(sequence,cardinality) = line.split("\\|")
      
      val support = cardinality.trim().toInt
      val itemsets = sequence.trim().split("-1").map(itemset => itemset.trim().split(" ").map(_.toInt).toList).toList

      new FSMPattern(support,itemsets)
      
    }).toList
          
    /* Put patterns to PatternCache */
    PatternCache.add(uid,patterns)
          
    /* Update JobCache */
    JobCache.add(uid,FSMStatus.FINISHED)

  }  
  
  private def parameters():Double = {
      
    try {
      return jobConf.get("support").get.asInstanceOf[Double]
        
    } catch {
      case e:Exception => {
         return 0.0          
      }
    }
    
  }
}