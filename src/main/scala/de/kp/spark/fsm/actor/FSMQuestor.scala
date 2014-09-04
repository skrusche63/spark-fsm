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

import akka.actor.{Actor,ActorLogging}

import de.kp.spark.fsm.Configuration
import de.kp.spark.fsm.model._

import de.kp.spark.fsm.util.{PatternCache,RuleCache}

class FSMQuestor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {

    case req:FSMRequest => {
      
      val origin = sender    
      
      val (uid,task) = (req.uid,req.task)
      task match {
        
        case "predict" => {

          val resp = if (RuleCache.exists(uid) == false) {           
            val message = FSMMessages.RULES_DO_NOT_EXIST(uid)
            new FSMResponse(uid,Some(message),None,None,None,FSMStatus.FAILURE)
            
          } else {    
             
            val antecedent = req.itemset.getOrElse(null)
             if (antecedent == null) {
               val message = FSMMessages.ANTECEDENTS_DO_NOT_EXIST(uid)
               new FSMResponse(uid,Some(message),None,None,None,FSMStatus.FAILURE)
             
             } else {
            
              val consequent = RuleCache.consequent(uid,antecedent)
              new FSMResponse(uid,None,None,None,Some(consequent),FSMStatus.SUCCESS)
             
             }
            
          }
           
          origin ! FSMModel.serializeResponse(resp)
        }

        case "patterns" => {
          /*
           * Patterns MUST exist then return computed patterns
           */
          val resp = if (PatternCache.exists(uid) == false) {           
            val message = FSMMessages.PATTERNS_DO_NOT_EXIST(uid)
            new FSMResponse(uid,Some(message),None,None,None,FSMStatus.FAILURE)
            
          } else {            
            val patterns = PatternCache.patterns(uid)
            new FSMResponse(uid,None,Some(patterns),None,None,FSMStatus.SUCCESS)
            
          }
           
          origin ! FSMModel.serializeResponse(resp)
        }
        
        case "rules" => {
          /*
           * Rules MUST exist then return computed rules
           */
          val resp = if (RuleCache.exists(uid) == false) {           
            val message = FSMMessages.RULES_DO_NOT_EXIST(uid)
            new FSMResponse(uid,Some(message),None,None,None,FSMStatus.FAILURE)
            
          } else {            
            val rules = RuleCache.rules(uid)
            new FSMResponse(uid,None,None,Some(rules),None,FSMStatus.SUCCESS)
            
          }
           
          origin ! FSMModel.serializeResponse(resp)
           
        }
        
      }
      
    }
  
  }
  
}