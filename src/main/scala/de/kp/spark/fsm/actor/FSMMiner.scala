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
import akka.actor.{ActorRef,Props}

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.fsm.Configuration
import de.kp.spark.fsm.model._

class FSMMiner(@transient val sc:SparkContext) extends BaseTrainer(Configuration) {

  protected def validate(req:ServiceRequest):Option[String] = {

    val uid = req.data(Names.REQ_UID)
    
    if (cache.statusExists(req)) {            
      return Some(Messages.TASK_ALREADY_STARTED(uid))   
    }
            
    req.data.get(Names.REQ_ALGORITHM) match {
        
      case None => {
        return Some(Messages.NO_ALGORITHM_PROVIDED(uid))              
      }
        
      case Some(algorithm) => {
        if (Algorithms.isAlgorithm(algorithm) == false) {
          return Some(Messages.ALGORITHM_IS_UNKNOWN(uid,algorithm))    
        }
          
      }
    
    }  
    
    req.data.get(Names.REQ_SOURCE) match {
        
      case None => {
        return Some(Messages.NO_SOURCE_PROVIDED(uid))       
      }
        
      case Some(source) => {
        if (Sources.isSource(source) == false) {
          return Some(Messages.SOURCE_IS_UNKNOWN(uid,source))    
        }          
      }
        
    }

    None
    
  }

  protected def actor(req:ServiceRequest):ActorRef = {

    val algorithm = req.data(Names.REQ_ALGORITHM)
    if (algorithm == Algorithms.SPADE) {      
      context.actorOf(Props(new SPADEActor(sc)))      
      
    } else {
       context.actorOf(Props(new TSRActor(sc)))
    
    }
    
  }

}