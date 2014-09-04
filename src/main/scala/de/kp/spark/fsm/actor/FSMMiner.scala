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
import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.fsm.Configuration

import de.kp.spark.fsm.model._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class FSMMiner extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  private val algorithmSupport = Array(FSMAlgorithms.SPADE,FSMAlgorithms.TSR)
  
  def receive = {

    case req:FSMRequest => {
      
      val origin = sender    
      
      val (uid,task) = (req.uid,req.task)
      task match {
        
        case "start" => {
         
        }
       
        case "status" => {
           
        }
        
        case _ => {
           
        }
        
      }
      
    }
    
    case _ => {}
  
  }

}