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

import de.kp.spark.fsm.model._
import de.kp.spark.fsm.sink.RedisSink

class FSMQuestor extends BaseActor {

  implicit val ec = context.dispatcher
  val sink = new RedisSink()
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)
      
      val Array(task,topic) = req.task.split(":")
      topic match {
        
        case "antecedent" => {

          val resp = if (sink.rulesExist(uid) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {    

            if (req.data.contains(Names.REQ_ITEMS) == false) {
               failure(req,Messages.NO_ITEMS_PROVIDED(uid))
             
             } else {

               val items = req.data(Names.REQ_ITEMS).split(",").map(_.toInt).toList
               val rules = sink.rulesByAntecedent(uid,items)
               
               val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> rules)
               new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
             
             }
            
          }
           
          origin ! resp
          context.stop(self)
          
        }
        
        case "consequent" => {

          val resp = if (sink.rulesExist(uid) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {    

            if (req.data.contains(Names.REQ_ITEMS) == false) {
               failure(req,Messages.NO_ITEMS_PROVIDED(uid))
             
             } else {

               val items = req.data(Names.REQ_ITEMS).split(",").map(_.toInt).toList
               val rules = sink.rulesByConsequent(uid,items)
               
               val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> rules)
               new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
             
             }
            
          }
           
          origin ! resp
          context.stop(self)
          
        }

        case "pattern" => {

          val resp = if (sink.patternsExist(uid) == false) {           
            failure(req,Messages.PATTERNS_DO_NOT_EXIST(uid))
            
          } else {            
            
            val patterns = sink.patterns(uid)
               
            val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> patterns)
            new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
            
          }
           
          origin ! resp
          context.stop(self)
          
        }
        
        case "get:rule" => {
          
          val resp = if (sink.rulesExist(uid) == false) {           
            failure(req,Messages.RULES_DO_NOT_EXIST(uid))
            
          } else {            
            
            val rules = sink.rules(uid)
               
            val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> rules)
            new ServiceResponse(req.service,req.task,data,ResponseStatus.SUCCESS)
            
          }
           
          origin ! resp
          context.stop(self)
          
        }
    
        case _ => {
      
          val origin = sender               
          val msg = Messages.REQUEST_IS_UNKNOWN()          
          
          origin ! failure(null,msg)
          context.stop(self)

        }
        
      }
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }
  
}